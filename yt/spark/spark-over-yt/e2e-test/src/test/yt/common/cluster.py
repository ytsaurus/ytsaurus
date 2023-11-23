from spyt.enabler import SpytEnablers
from spyt.standalone import start_spark_cluster, SparkDefaultArguments
from spyt.submit import java_gateway, SparkSubmissionClient

from yt.common import YtError
from yt.wrapper import YtClient

from contextlib import contextmanager


class SpytCluster(object):
    def __init__(self, proxy, discovery_path="//home/cluster", java_home=None):
        self.proxy = proxy
        self.discovery_path = discovery_path
        self.java_home = java_home
        self.user = "root"
        self.token = ""

    @staticmethod
    def get_params():
        params = SparkDefaultArguments.get_params()
        params["operation_spec"]["max_failed_job_count"] = 1
        return params

    @staticmethod
    def get_enablers():
        return SpytEnablers()

    def __enter__(self):
        yt_client = YtClient(proxy=self.proxy)
        self.op = start_spark_cluster(
            worker_cores=2, worker_memory='3G', worker_num=1, worker_cores_overhead=None, worker_memory_overhead='512M',
            operation_alias='integration_tests', discovery_path=self.discovery_path,
            master_memory_limit='3G', enable_history_server=False, params=SpytCluster.get_params(), enable_tmpfs=False,
            enablers=SpytCluster.get_enablers(), client=yt_client, enable_livy=False)
        if self.op is None:
            raise YtError("Cluster starting failed")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.op.complete()
        except YtError as err:
            inner_errors = [err]
            if exc_type is not None:
                inner_errors.append(exc_val)
            raise YtError("Cluster stopping failed", inner_errors=inner_errors)

    @contextmanager
    def submission_client(self):
        with java_gateway(java_home=self.java_home) as gw:
            yield SparkSubmissionClient(gw, self.proxy, self.discovery_path, "1.0.0", self.user, self.token)

    def submit_cluster_job(self, job_path, conf=None, args=None):
        conf = conf or {}
        args = args or []
        with self.submission_client() as client:
            launcher = client.new_launcher()
            launcher.set_app_resource("yt:/" + job_path)
            launcher.add_app_args(*args)
            for key, value in conf.items():
                launcher.set_conf(key, value)
            app_id = client.submit(launcher)
            status = client.wait_final(app_id)
            return status
