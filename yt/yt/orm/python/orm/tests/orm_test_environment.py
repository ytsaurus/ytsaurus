from __future__ import print_function

from .helpers import yatest_common

from yt.orm.admin.db_manager import DbManager
from yt.orm.library.common import wait
from yt.orm.library.dynamic_config import (
    OrmDynamicConfig,
    WaitType,
)
from yt.orm.library.orchid_client import OrmOrchidClient

from yt.orm.client.client import OrmClient  # noqa: F401
from yt.orm.local.local import OrmInstance  # noqa: F401
from yt.environment import arcadia_interop
from yt.wrapper.common import generate_uuid, update
from yt.wrapper import YtClient, yson  # noqa: F401


from abc import ABCMeta, abstractmethod
from http.server import HTTPServer
from multiprocessing.pool import ThreadPool


import contextlib
import copy
import enum
import os
import requests
import socket
import sys
import threading


TO_INITIAL_DB_VERSION = -1


def _try_makedirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


class SandboxBase:
    def __init__(self, test_environment):
        self._name = test_environment.DATA_MODEL_TRAITS.get_snake_case_name()
        self._sandbox_path_from_os_environment = getattr(test_environment, "SANDBOX_PATH_FROM_OS_ENVIRONMENT", False)
        self._tests_location = test_environment.TESTS_LOCATION
        self._binary_relative_paths = test_environment.BINARY_RELATIVE_PATHS
        self._environment_id = getattr(test_environment, "_id", None) or generate_uuid()
        self._logger = test_environment.logger
        self._tests_base_path = self.infer_tests_base_path(self._tests_location)
        _try_makedirs(self.get_port_locks_path())

    @staticmethod
    def infer_tests_base_path(tests_location):
        if yatest_common is None:
            return os.environ.get("TESTS_SANDBOX", tests_location + ".sandbox")
        else:
            return arcadia_interop.get_output_path()

    @staticmethod
    def make_yt_manager_dir(environment_cls):
        yt_path = os.path.join(
            SandboxBase.infer_tests_base_path(environment_cls.TESTS_LOCATION),
            environment_cls.DATA_MODEL_TRAITS.get_snake_case_name() + "_yt_" + generate_uuid(),
        )
        _try_makedirs(yt_path)
        return yt_path

    def get_port_locks_path(self):
        return os.path.join(self._tests_base_path, "ports")

    # Must give priority to #path.
    def _insert_environ_path(self, path):
        assert len(path) > 0
        tokens = os.environ.get("PATH", "").split(os.pathsep)
        if len(tokens) > 0 and tokens[0] == path:
            self._logger.info(
                "Environment PATH variable already contains the given value as the first token with the highest priority (value: {})".format(
                    path
                )
            )
        else:
            self._logger.info("Prepending value to environment PATH variable (value: {})".format(path))
            os.environ["PATH"] = os.pathsep.join([path, os.environ.get("PATH", "")])

    def prepare_sandbox(self):
        if yatest_common is not None:
            # Generate unique destinations to isolate them from each other.
            #
            # Though local `ya make -A` provides a separate working directory for each test chunk,
            # YA_MAKE Sandbox task with Ya build type provides the only one, which requires isolation.
            #
            # These directories are expected to be lightweight, mostly containing symbolic links.
            # Do not use ram drive, because symbolic link across file systems is forbidden, which forces costly copying.
            destination = yatest_common.work_path("build_" + self._environment_id)
            os.makedirs(destination)

            if self._sandbox_path_from_os_environment:
                yt_tests_package_directory_path = os.environ.get("YT_TESTS_PACKAGE_DIR")
            else:
                yt_tests_package_directory_path = "yt/packages/latest"

            self._logger.info(
                "Preparing YT environment (destination: {}, yt_tests_package: {})".format(
                    destination,
                    yt_tests_package_directory_path,
                )
            )
            if yt_tests_package_directory_path:
                self._yt_env_path = arcadia_interop.prepare_yt_environment(
                    destination,
                    package_dir=yt_tests_package_directory_path,
                )
            else:
                self._yt_env_path = arcadia_interop.prepare_yt_environment(destination)

            for binary_relative_path in self._binary_relative_paths:
                self.create_symlink_for_binary(binary_relative_path)

            # Gives priority to #yt_env_path.
            self._insert_environ_path(self._yt_env_path)

        self._make_sandbox(self._name)

    def _make_sandbox(self, name):
        self.path = os.path.join(self._tests_base_path, name + "_" + self._environment_id)
        os.makedirs(self.path)

    def create_symlink_for_binary(self, binary_relative_path):
        try:
            binary_path = yatest_common.binary_path(binary_relative_path)
        except Exception:
            self._logger.info(
                "Failed to locate server binary, probably it is missing in DEPENDS section (binary: {})".format(
                    binary_relative_path,
                )
            )
            return
        destination = os.path.join(self._yt_env_path, os.path.basename(binary_relative_path))
        self._logger.info(
            "Creating server binary symbolic link (source: {}, destination: {})".format(
                binary_path,
                destination,
            )
        )
        os.symlink(binary_path, destination)

    def add_sandbox(self, name):
        added_sandbox = copy.copy(self)
        added_sandbox._make_sandbox(name)
        return added_sandbox

    def save_sandbox(self):
        if yatest_common is not None:
            arcadia_interop.save_sandbox(self.path, os.path.basename(self.path))


class HistoryMigrationState(str, enum.Enum):
    INITIAL = "initial"
    WRITE_BOTH = "write_both"
    AFTER_BACKGROUND_MIGRATION = "after_background_migration"
    READ_NEW = "read_new"
    REVOKE_OLD_TOKENS = "revoke_old_tokens"
    TARGET = "target"


class DummyTestClass:
    pass


class OrmTestEnvironment(object):
    """
    Wrapper providing API for managing single local ORM Master and undelying local YT.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_orm_master_config_template(self):
        raise NotImplementedError

    def get_orm_client_config_template(self):
        return dict()

    @abstractmethod
    def get_orm_master_instance_cls(self):
        raise NotImplementedError

    @abstractmethod
    def reset_orm(self):
        raise NotImplementedError

    def __init__(
        self,
        test_cls,
        yt_manager=None,
        local_yt_options=None,
        orm_master_config=None,
        test_sandbox=None,
        id=None,
        **kwargs
    ):
        self._id = id
        self._enable_ssl = getattr(test_cls, "ENABLE_SSL", False)
        self._avoid_migrations = getattr(test_cls, "AVOID_MIGRATIONS", True)
        self._db_version = getattr(test_cls, "DB_VERSION", self.DATA_MODEL_TRAITS.get_actual_db_version())
        if local_yt_options is None:
            local_yt_options = getattr(test_cls, "LOCAL_YT_OPTIONS", None)
        self._local_yt_options = local_yt_options

        if self._db_version == TO_INITIAL_DB_VERSION:
            self._db_version = self.DATA_MODEL_TRAITS.get_initial_db_version()
        assert not self._avoid_migrations or self._db_version == self.DATA_MODEL_TRAITS.get_actual_db_version()

        snake_case_name = self.DATA_MODEL_TRAITS.get_snake_case_name()
        upper_snake_case_name = snake_case_name.upper()

        if orm_master_config is None:
            orm_master_yson_config = os.environ.get("{}_MASTER_CONFIG_YSON".format(upper_snake_case_name), None)
            orm_master_config = update(
                yson.loads(orm_master_yson_config.encode("utf-8")) if orm_master_yson_config else None,
                getattr(test_cls, "{}_MASTER_CONFIG".format(upper_snake_case_name), None),
            )

        orm_master_config = update(self.get_orm_master_config_template(), orm_master_config)

        if self._local_yt_options is None:
            self._local_yt_options = dict()

        if test_sandbox is None:
            test_sandbox = SandboxBase(self)
        self._test_sandbox = test_sandbox
        self._test_sandbox.prepare_sandbox()

        self._dynamic_config = None
        self._orchid_client = None

        orm_instance_options = dict(
            enable_ssl=self._enable_ssl,
            local_yt_options=self._local_yt_options,
            port_locks_path=self._test_sandbox.get_port_locks_path(),
            use_native_connection="{}_USE_NATIVE_CONNECTION".format(upper_snake_case_name) in os.environ,
            use_federated_connection="{}_USE_FEDERATED_CONNECTION".format(upper_snake_case_name) in os.environ,
            db_version=self._db_version,
            avoid_migrations=self._avoid_migrations,
        )
        orm_instance_options["{}_master_config".format(snake_case_name)] = orm_master_config
        orm_instance = self.get_orm_master_instance_cls()(
            path=self._test_sandbox.path,
            yt_manager=yt_manager,
            options=update(orm_instance_options, kwargs),
        )
        setattr(self, "{}_instance".format(snake_case_name), orm_instance)

        self.prepare()
        self._clean = True
        self._started = False

    def prepare(self):
        self.orm_instance.prepare()
        setattr(self, "{}_client".format(self.DATA_MODEL_TRAITS.get_snake_case_name()), None)
        self.yt_client = self.orm_instance.create_yt_client()  # type: YtClient
        self.db_manager = DbManager(
            self.yt_client,
            self.DATA_MODEL_TRAITS.get_default_path(),
            self.DATA_MODEL_TRAITS,
            self._db_version,
        )

    def start(self, *args, **kwargs):
        if self._started:
            return

        try:
            snake_case_name = self.DATA_MODEL_TRAITS.get_snake_case_name()
            self.orm_instance.start(*args, **kwargs)
            setattr(self, "{}_client".format(snake_case_name), self.create_client())
            self._started = True
        except Exception:
            self._test_sandbox.save_sandbox()
            raise

    def stop(self):
        if not self._started:
            return

        try:
            if self.orm_client is not None:
                self.orm_client.close()
            setattr(self, "{}_client".format(self.DATA_MODEL_TRAITS.get_snake_case_name()), None)

            self.orm_instance.stop()
            self._started = False
            self._test_sandbox.save_sandbox()
        except Exception:
            # Additional logging added due to https://github.com/pytest-dev/pytest/issues/2237
            self.logger.exception("Failed to stop {} test environment".format(
                self.DATA_MODEL_TRAITS.get_human_readable_name(),
            ))
            raise

    def stop_master(self):
        if self.orm_client is not None:
            self.orm_client.close()
            setattr(self, "{}_client".format(self.DATA_MODEL_TRAITS.get_snake_case_name()), None)
        self.orm_instance.stop_master()
        self._started = False

    def setup_test(self, reset_db):
        print("\n", file=sys.stderr)
        try:
            if not self._clean and reset_db:
                self.reset_orm()
                self.reset_cypress_config_patch()
                self._clean = True
        except Exception:
            # Additional logging added due to https://github.com/pytest-dev/pytest/issues/2237
            self.logger.exception("Failed to setup {} test environment for a test".format(
                self.DATA_MODEL_TRAITS.get_human_readable_name(),
            ))
            raise

    def teardown_test(self):
        print("\n", file=sys.stderr)
        self._clean = False

    def create_client(self, config=None, **kwargs):
        client_config = update(self.get_orm_client_config_template(), config)
        return self.orm_instance.create_client(config=client_config, **kwargs)

    def add_sandbox(self, name):
        return self._test_sandbox.add_sandbox(name)

    def get_db_path(self):
        return self.orm_instance.get_db_path()

    def get_db_version(self):
        return self.orm_instance.get_db_version()

    def get_cypress_config_patch(self, inner_path=None):
        return self.dynamic_config.get_config_patches(inner_path)

    def set_cypress_config_patch(self, value, type="document", wait=WaitType.Leader):
        return self.dynamic_config.set_config("/", value, wait=wait, type=type)

    def update_cypress_config_patch(self, patch, wait=WaitType.Leader):
        return self.dynamic_config.update_config(patch, wait=wait)

    def set_cypress_config_patch_in_context(self, value, wait=WaitType.Leader):
        return self.dynamic_config.with_config(value, wait=wait)

    def reset_cypress_config_patch(self, wait=WaitType.Leader):
        return self.dynamic_config.reset_config_patches(wait=wait)

    @property
    def dynamic_config(self):  # type: () -> OrmDynamicConfig
        if not self._dynamic_config:
            self._dynamic_config = OrmDynamicConfig(
                self.yt_client,
                self.DATA_MODEL_TRAITS.get_default_path(),
            )
        return self._dynamic_config

    @property
    def orm_instance(self):  # type: () -> OrmInstance
        snake_case_name = self.DATA_MODEL_TRAITS.get_snake_case_name()
        return getattr(self, "{}_instance".format(snake_case_name))

    @property
    def orm_client(self):  # type: () -> OrmClient
        snake_case_name = self.DATA_MODEL_TRAITS.get_snake_case_name()
        return getattr(self, "{}_client".format(snake_case_name))

    @property
    def orchid_client(self):  # type: () -> OrmOrchidClient
        if not self._orchid_client:
            self._orchid_client = OrmOrchidClient(
                self.yt_client,
                self.DATA_MODEL_TRAITS.get_default_path(),
                "master"
            )
        return self._orchid_client

    @property
    def client(self):  # type: () -> OrmClient
        return self.orm_client

    @property
    def main_environment(self):
        return self

    def get_table_rows(self, table_name):
        return list(self.yt_client.select_rows("* from [{}]".format(table_name)))

    def clear_table(self, table_name, is_key_field=None):
        if is_key_field is None:
            fields = self.yt_client.get(table_name + "/@schema")

            def deduce_is_key(field):
                for field_data in fields:
                    if field_data["name"] == field:
                        return "sort_order" in field_data and "expression" not in field_data
                raise Exception("Unexpected table field {}".format(field))

            is_key_field = deduce_is_key

        rows_to_delete = [{key: row[key] for key in row if is_key_field(key)} for row in self.get_table_rows(table_name)]
        if rows_to_delete:
            self.yt_client.delete_rows(table_name, rows_to_delete)

    def set_history_migration_state(self, state):  # type: (OrmTestEnvironment, HistoryMigrationState) -> None
        return self.dynamic_config.update_config({"transaction_manager": {"history_migration_state": state.value}})


class OrmTestMultienvironment(object):
    """
    Wrapper providing API for managing multiple independent local ORM Masters, each with
    its own underlying local YT.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_single_environment_cls(self):
        raise NotImplementedError

    def __init__(self, test_cls, *args, **kwargs):
        self._thread_pool = ThreadPool(5)
        clusters = getattr(test_cls, "CLUSTERS", [])
        if not clusters:
            raise ValueError("List of clusters must be specified")
        if len(set(clusters)) != len(clusters):
            raise ValueError("Clusters must be unique")
        for cluster in clusters:
            if not cluster:
                raise ValueError("Cluster cannot be empty")

        self.main_cluster = getattr(test_cls, "MAIN_CLUSTER", None)
        if not self.main_cluster:
            raise ValueError("Main cluster must me specified")
        if self.main_cluster not in clusters:
            raise ValueError("Unknown main cluster \"{}\"".format(self.main_cluster))

        environment_cls = self.get_single_environment_cls()
        self.cluster_to_environment = {
            cluster: environment_cls(test_cls, id=cluster, *args, **kwargs)
            for cluster in clusters
        }
        self.main_environment = self.cluster_to_environment[self.main_cluster]

    def start(self):
        self._execute_for_each_env(lambda env: env.start())
        self._update_orm_clients()

    def stop(self):
        self._execute_for_each_env(lambda env: env.stop())
        self._update_orm_clients()

    def setup_test(self, reset_db):
        self._execute_for_each_env(lambda env: env.setup_test(reset_db))

    def teardown_test(self):
        self._execute_for_each_env(lambda env: env.teardown_test())

    def _execute_for_each_env(self, action):
        self._thread_pool.map(action, self.cluster_to_environment.values())

    def _update_orm_clients(self):
        orm_name = self.get_single_environment_cls().DATA_MODEL_TRAITS.get_snake_case_name()
        cluster_to_orm_client = {
            cluster: environment.orm_client
            for cluster, environment in self.cluster_to_environment.items()
        }
        setattr(self, "cluster_to_{}_client".format(orm_name), cluster_to_orm_client)
        setattr(self, "main_{}_client".format(orm_name), self.main_environment.orm_client)


class MockServer:
    def __init__(self, handler):
        if self.is_ipv6_enabled():
            class HTTPServerV6(HTTPServer):
                address_family = socket.AF_INET6

            self._server = HTTPServerV6(("::", 0), handler)
        else:
            self._server = HTTPServer(("localhost", 0), handler)

        self.port = self._server.server_address[1]

    def get_address(self):
        return "http://localhost:{}".format(self.port)

    def start(self):
        th = threading.Thread(target=self._server.serve_forever)
        th.daemon = True
        th.start()

    def start_and_wait_for_readiness(self):
        self.start()
        wait(lambda: self.ping(), ignore_exceptions=True, iter=20)

    def stop(self):
        self._server.shutdown()

    def ping(self):
        rsp = requests.get("{}/ping".format(self.get_address()))
        return rsp.ok

    def is_ipv6_enabled(self):
        return False


class OrmMicroserviceTestEnvironment(object):
    """
    Wrapper providing API for managing one or several instances of local
    ORM microservice and shared undelying environment, which can be
    either single or multi.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_binary_relative_paths(self):
        raise NotImplementedError

    @abstractmethod
    def get_microservice_snake_case_name(self):
        raise NotImplementedError

    @abstractmethod
    def get_microservice_instance_cls(self):
        raise NotImplementedError

    @abstractmethod
    def start_microservice(self, microservice_instance):
        raise NotImplementedError

    def __init__(self, test_cls, orm_env):
        self._thread_pool = ThreadPool(5)
        setattr(self, self.orm_snake_case_name, orm_env)

        instance_count = getattr(
            test_cls,
            "{}_INSTANCE_COUNT".format(self.microservice_full_name.upper()),
            1,
        )
        microservice_instances = [
            self._create_microservice_instance(test_cls, instance_tag)
            for instance_tag in range(0, instance_count)
        ]
        setattr(
            self,
            self.plural_microservice_instance_name,
            microservice_instances,
        )

        if instance_count == 1:
            setattr(self, self.microservice_instance_name, microservice_instances[0])

        self._prepare()

    def _create_microservice_instance(self, test_cls, instance_tag):
        return self.get_microservice_instance_cls()(
            working_directory_path=os.path.join(
                self.orm_env.main_environment._test_sandbox.path,
                self.microservice_full_name,
                "instance_{}".format(instance_tag),
            ),
            config_patch=getattr(
                test_cls,
                "{}_CONFIG".format(self.microservice_full_name.upper()),
                None,
            ),
            dynamic_config_patch=getattr(
                test_cls,
                "{}_DYNAMIC_CONFIG".format(self.microservice_full_name.upper()),
                None,
            ),
            instance_tag=instance_tag,
            enable_debug_logging=True,
        )

    def _prepare(self):
        for path in self.get_binary_relative_paths():
            self.orm_env.main_environment._test_sandbox.create_symlink_for_binary(path)

        self._clean = True

    def start(self):
        self.orm_env.start()
        self._execute_for_each_microservice_instance(self.start_microservice)

    def stop(self):
        self._execute_for_each_microservice_instance(
            lambda instance: instance.stop(),
        )
        self.orm_env.stop()

    def setup_test(self, reset_db):
        self.orm_env.setup_test(reset_db)
        if not self._clean and reset_db:
            self._execute_for_each_microservice_instance(
                lambda instance: instance.reset_dynamic_config_to_initial(),
            )

        self._clean = True

    def teardown_test(self):
        self._clean = False
        self.orm_env.teardown_test()

    def _execute_for_each_microservice_instance(self, action):
        self._thread_pool.map(action, self.microservice_instances)

    @property
    def orm_snake_case_name(self):
        return self.DATA_MODEL_TRAITS.get_snake_case_name()

    @property
    def microservice_full_name(self):
        return "{}_{}".format(
            self.orm_snake_case_name,
            self.get_microservice_snake_case_name(),
        )

    @property
    def microservice_instance_name(self):
        return "{}_instance".format(self.microservice_full_name)

    @property
    def plural_microservice_instance_name(self):
        return "{}s".format(self.microservice_instance_name)

    @property
    def orm_env(self):
        return getattr(self, self.orm_snake_case_name)

    @property
    def microservice_instances(self):
        return getattr(self, self.plural_microservice_instance_name)


class OrmSimpleMicroserviceTestEnvironment(OrmMicroserviceTestEnvironment):
    """
    Same as OrmMicroserviceTestEnvironment, but assumes that ORM microservice only interacts
    with single ORM environment (cluster). Underlying ORM environment can be multiple, however,
    in that case only main ORM subenvironment will be accessible to the microservice.
    """
    def start_microservice(self, microservice_instance):
        main_orm_env = self.orm_env.main_environment
        return microservice_instance.start(
            yt_http_proxy_address=main_orm_env.orm_instance.yt_instance.get_http_proxy_address(),
            port_locks_path=main_orm_env._test_sandbox.get_port_locks_path(),
            orm_master_grpc_address=main_orm_env.orm_instance.orm_client_grpc_address,
        )


def create_test_suite_environment(environment_cls, request, *args, **kwargs):
    request_cls = getattr(request, "cls", DummyTestClass)
    environment = environment_cls(request_cls, *args, **kwargs)
    if getattr(request_cls, "START", True):
        environment.start()

    request.addfinalizer(lambda: environment.stop())
    return environment


def create_test_suite_orm_environment_for_microservices(orm_environment_cls, request):
    return create_test_suite_environment(
        orm_environment_cls,
        request,
        local_yt_options=dict(
            http_proxy_count=1,
            rpc_proxy_count=1,
        ),
    )


def create_single_test_environment(request, test_suite_environment):
    reset_db = getattr(request.cls, "RESET_DB_BEFORE_TEST", True)
    test_suite_environment.setup_test(reset_db)
    if not getattr(request.cls, "NO_TEARDOWN", False):
        request.addfinalizer(lambda: test_suite_environment.teardown_test())

    return test_suite_environment


@contextlib.contextmanager
def create_standalone_orm_test_environment(orm_environment_cls, request, *args, **kwargs):
    environment = create_test_suite_environment(orm_environment_cls, request, start=True, *args, **kwargs)
    environment.setup_test(reset_db=False)
    yield environment
    environment.teardown_test()
    environment.stop()
