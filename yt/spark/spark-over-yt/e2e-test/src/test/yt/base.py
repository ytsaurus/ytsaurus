from spyt.enabler import SpytEnablers
from spyt.standalone import start_spark_cluster, SparkDefaultArguments
from spyt.submit import java_gateway, SparkSubmissionClient

from yt_commands import (get, set, exists, sync_create_cells, print_debug, create, create_user, issue_token,
                         write_local_file)
from yt_env_setup import YTEnvSetup

from yt.common import YtError
import yt.wrapper

from contextlib import contextmanager
from importlib import import_module
import os
import yatest.common


class SpytTestBase(YTEnvSetup):
    NUM_MASTERS = 1
    NUM_NODES = 3
    NUM_SCHEDULERS = 1
    NUM_DISCOVERY_SERVERS = 1
    USE_DYNAMIC_TABLES = True

    ENABLE_HTTP_PROXY = True
    ENABLE_RPC_PROXY = True

    DELTA_MASTER_CONFIG = {
        "cypress_manager": {
            "default_table_replication_factor": 1,
            "default_file_replication_factor": 1,
        }
    }

    DELTA_NODE_CONFIG = {
        "exec_node": {
            "job_controller": {
                "resource_limits": {
                    "cpu": 2,
                    "memory": 4 * 2 ** 30,
                },
            },
        }
    }

    PYTHON_VERSION = "3.11"  # Current arcadia's python
    DEFAULT_PYTHON_PATH = "python3.11"  # This command will be used for running python tasks on workers
    JAVA_HOME = yatest.common.runtime.global_resources()['JDK11_RESOURCE_GLOBAL']
    SPYT_ROOT_PATH = "//home/spark"

    SPARK_USER = None
    SPARK_TOKEN = None

    @classmethod
    def get_rpc_proxy_address(cls):
        return "http://" + cls.Env.get_rpc_proxy_address()

    @classmethod
    def get_proxy_address(cls):
        return "http://" + cls.Env.get_http_proxy_address()

    @staticmethod
    def _signal_instance(pid, signal_number):
        print_debug("Killing instance with with os.kill({}, {})".format(pid, signal_number))
        os.kill(pid, signal_number)

    @classmethod
    def _create_cypress_structure(cls):
        build_path = yatest.common.build_path("yt/spark/spark-over-yt/e2e-test/src/test/yt/data")
        sources_path = os.path.join(build_path, "spyt_cluster")
        print_debug(f"All resources in build directory {build_path}")

        # Dash usage workaround
        module_path = "yt.spark.spark-over-yt.tools.release.publisher"
        config_generator = import_module(module_path + ".config_generator")
        local_manager = import_module(module_path + ".local_manager")
        publish_cluster = import_module(module_path + ".publish_cluster")
        remote_manager = import_module(module_path + ".remote_manager")

        yt_client = yt.wrapper.YtClient(proxy=cls.Env.get_proxy_address())
        client_builder = remote_manager.ClientBuilder(cls.SPYT_ROOT_PATH, yt_client)
        default_version = local_manager.PackedVersion({"scala": "1.0.0"})
        uploader = remote_manager.Client(client_builder)
        versions = local_manager.Versions(default_version, default_version, default_version)
        config_generator.make_configs(sources_path, client_builder, versions, os_release=True)
        print_debug("Config files generated")

        publish_conf = remote_manager.PublishConfig()
        publish_cluster.upload_spark_fork(uploader, versions, sources_path, publish_conf)
        publish_cluster.upload_cluster(uploader, versions, sources_path, publish_conf)
        publish_cluster.upload_client(uploader, versions, sources_path, publish_conf)
        print_debug("Cluster files uploaded")

        python_path = yatest.common.get_param('python_path', default=cls.DEFAULT_PYTHON_PATH)
        print_debug(f"Set python interpreter: {python_path}")
        print_debug(f"Set java home: {cls.JAVA_HOME}")

        python_cluster_paths = {cls.PYTHON_VERSION: python_path}
        set("//home/spark/conf/global/python_cluster_paths", python_cluster_paths)
        set("//home/spark/conf/global/environment/JAVA_HOME", cls.JAVA_HOME)
        spark_conf = get("//home/spark/conf/global/spark_conf")
        spark_conf['spark.executor.cores'] = '1'
        spark_conf['spark.executor.memory'] = '1g'
        spark_conf['spark.pyspark.python'] = python_path
        set("//home/spark/conf/global/spark_conf", spark_conf)

    @classmethod
    def setup_class(cls, test_name=None, run_id=None):
        super().setup_class(test_name=test_name, run_id=run_id)

        if exists(cls.SPYT_ROOT_PATH):
            return
        print_debug("No SPYT root found, creating new")
        cls._create_cypress_structure()
        SpytCluster.java_home = cls.JAVA_HOME
        SpytCluster.proxy_address = cls.get_proxy_address()

    def setup_method(self, method):
        super().setup_method(method)
        sync_create_cells(1)

        self.SPARK_USER = "spark_user"
        create_user(self.SPARK_USER)
        self.SPARK_TOKEN, _ = issue_token(self.SPARK_USER)

    @staticmethod
    def upload_job_file(source_path, remote_path):
        create("file", remote_path)
        write_local_file(remote_path, yatest.common.source_path(source_path))


class SpytCluster(object):
    java_home = None
    proxy_address = None
    discovery_path = "//home/cluster"

    def __init__(self, user, token):
        self.user = user
        self.token = token

    @staticmethod
    def get_params():
        params = SparkDefaultArguments.get_params()
        params["operation_spec"]["max_failed_job_count"] = 1
        return params

    @staticmethod
    def get_enablers():
        return SpytEnablers()

    def __enter__(self):
        client = yt.wrapper.YtClient(proxy=self.proxy_address, token=self.token)
        print_debug("Starting Spark cluster")
        self.op = start_spark_cluster(
            worker_cores=2, worker_memory='3G', worker_num=1, worker_cores_overhead=None, worker_memory_overhead='512M',
            operation_alias='integration_tests', discovery_path=self.discovery_path,
            master_memory_limit='3G', enable_history_server=False, params=SpytCluster.get_params(), enable_tmpfs=False,
            enablers=SpytCluster.get_enablers(), client=client, enable_livy=False)
        if self.op is None:
            raise YtError("Cluster starting failed")
        print_debug("Spark cluster started")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print_debug("Stopping Spark cluster", self.op.id)
        try:
            self.op.complete()
            print_debug(self.op.__dir__)
        except YtError as err:
            inner_errors = [err]
            if exc_type is not None:
                inner_errors.append(exc_val)
            raise YtError("Cluster stopping failed", inner_errors=inner_errors)

    @contextmanager
    def submission_client(self):
        with java_gateway(java_home=self.java_home) as gw:
            yield SparkSubmissionClient(gw, self.proxy_address, self.discovery_path, "1.0.0", self.user, self.token)

    def submit_cluster_job(self, job_path, conf=None, args=None):
        conf = conf or {}
        args = args or []
        print_debug("Starting java gateway")
        with self.submission_client() as client:
            launcher = client.new_launcher()
            launcher.set_app_resource("yt:/" + job_path)
            launcher.add_app_args(*args)
            for key, value in conf.items():
                launcher.set_conf(key, value)
            app_id = client.submit(launcher)
            status = client.wait_final(app_id)
            print_debug("Job finished")
            return status
