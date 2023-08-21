import argparse
import getpass
import logging
import os
import re
import subprocess

from spyt.dependency_utils import require_yt_client
require_yt_client()

from yt.wrapper import YPath
from yt.wrapper.cypress_commands import list as yt_list, create, exists
from yt.wrapper.errors import YtHttpResponseError
from yt.wrapper.http_helpers import get_proxy_url, get_user_name
from yt.wrapper.operation_commands import get_operation_url
from yt.yson.convert import yson_to_json
from spyt.conf import is_supported_cluster_minor_version

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


class SparkCluster(object):
    def __init__(self, master_endpoint, master_web_ui_url, master_rest_endpoint, operation_id, shs_url,
                 spark_cluster_version, children_operation_ids):
        self.master_endpoint = master_endpoint
        self.master_web_ui_url = master_web_ui_url
        self.master_rest_endpoint = master_rest_endpoint
        self.operation_id = operation_id
        self.shs_url = shs_url
        self.spark_cluster_version = spark_cluster_version
        self.children_operation_ids = children_operation_ids

    def operation_url(self, client=None):
        return get_operation_url(self.operation_id, client=client)

    def children_operation_urls(self, client=None):
        return [get_operation_url(id, client=client) for id in self.children_operation_ids]


class SparkDiscovery(object):
    def __init__(self, discovery_path=None):
        discovery_path = discovery_path or os.getenv("SPARK_BASE_DISCOVERY_PATH")
        self.base_discovery_path = YPath(discovery_path)

    @staticmethod
    def getAll(path, client=None):
        try:
            return yt_list(path, client=client)
        except YtHttpResponseError as e:
            logging.warning("Failed to get path {}, message: {}".format(path, e.message))
            for inner in e.inner_errors:
                logging.warning("Failed to get path {}, inner message {}".format(path, inner["message"]))
            raise e

    @staticmethod
    def get(path, client=None):
        all = SparkDiscovery.getAll(path, client)
        if not all:
            return None
        else:
            return all[0]

    @staticmethod
    def getOption(path, client=None):
        try:
            return SparkDiscovery.get(path, client)
        except YtHttpResponseError as _:
            return None

    @staticmethod
    def getOptions(path, client=None):
        try:
            return SparkDiscovery.getAll(path, client)
        except YtHttpResponseError as _:
            return []

    def create(self, client):
        create("map_node", self.discovery(), recursive=True, ignore_existing=True, client=client)
        create("map_node", self.logs(), recursive=True, ignore_existing=True, client=client)

    def discovery(self):
        return self.base_discovery_path.join("discovery")

    def operation(self):
        return self.discovery().join("operation")

    def children_operations(self):
        return self.discovery().join("children_operations")

    def logs(self):
        return self.base_discovery_path.join("logs")

    def event_log(self):
        return self.logs().join("event_log")

    def event_log_table(self):
        return self.logs().join("event_log_table")

    def worker_log(self):
        return self.logs().join("worker_log")

    def master_spark(self):
        return self.discovery().join("spark_address")

    def master_webui(self):
        return self.discovery().join("webui")

    def master_rest(self):
        return self.discovery().join("rest")

    def shs(self):
        return self.discovery().join("shs")

    def stderr(self):
        return self.logs().join("stderr")

    def spark_cluster_version(self):
        return self.discovery().join("version")

    def conf(self):
        return self.discovery().join("conf")

    def master_wrapper(self):
        return self.discovery().join("master_wrapper")


class ClusterInfo(object):
    def __init__(self, master, shs, workers, operation, workers_operation, multiop_mode, max_workers_count, user_slots):
        self.master = master
        self.shs = shs
        self.workers = workers
        self.operation = operation
        self.workers_operation = workers_operation
        self.multiop_mode = multiop_mode
        self.max_workers_count = max_workers_count
        self.user_slots = user_slots

    def workers_count(self):
        return len(self.workers)


def cluster_info(yt_client, discovery_path):
    discovery = SparkDiscovery(discovery_path)
    master = SparkDiscovery.getOption(discovery.master_spark())
    shs = SparkDiscovery.getOption(discovery.shs())
    operation = SparkDiscovery.getOption(discovery.operation())
    children = SparkDiscovery.getOptions(discovery.children_operations())
    if not children:
        worker_operation = operation
        multiop_mode = False
    else:
        worker_operation = children[0]
        multiop_mode = True
    yson_jobs = yt_client.list_jobs(worker_operation, job_state="running")
    jobs = yson_to_json(yson_jobs)['jobs']
    op = yson_to_json(yt_client.get_operation(worker_operation))
    resource_limits = op['runtime_parameters']['scheduling_options_per_pool_tree']['physical']['resource_limits']
    if 'user_slots' in resource_limits:
        user_slots = resource_limits['user_slots']
    else:
        user_slots = -1
    max_job_count = op['spec']['tasks']['workers']['job_count']

    def host(addr):
        h, _ = addr.split(':')
        return h

    workers = [j['address'] for j in jobs
               if 'address' in j and host(j['address']) != host(master) and host(j['address']) != host(shs)]
    return ClusterInfo(master, shs, workers, operation, worker_operation, multiop_mode, max_job_count, user_slots)


def parse_memory(memory):
    if isinstance(memory, int):
        return memory
    if memory is None:
        return None
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    m = re.match(r"(\d+)(.*)", memory)
    value = int(m.group(1))
    unit = m.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    return value * units[unit]


def format_memory(memory_bytes):
    if memory_bytes is None:
        return None
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_bytes % units["gb"] == 0:
        return "{}G".format(memory_bytes // units["gb"])
    if memory_bytes % units["mb"] == 0:
        return "{}M".format(memory_bytes // units["mb"])
    if memory_bytes % units["kb"] == 0:
        return "{}K".format(memory_bytes // units["kb"])
    return "{}B".format(memory_bytes)


def get_spark_master(discovery, rest, yt_client=None):
    master_path = discovery.master_rest() if rest else discovery.master_spark()
    master = SparkDiscovery.get(master_path, client=yt_client)
    return "spark://{0}".format(master)


def scala_buffer_to_list(buffer):
    return [buffer.apply(i) for i in range(buffer.length())]


def default_user():
    return os.getenv("YT_USER") or getpass.getuser()


def default_token():
    token = os.getenv("YT_TOKEN")
    if token is None:
        token_file = os.path.join(os.getenv("HOME"), ".yt", "token")
        if os.path.exists(token_file):
            with open(token_file) as f:
                token = f.readline().strip()
    return token


def base_spark_conf(client, discovery):
    yt_proxy = get_proxy_url(required=True, client=client)
    yt_user = get_user_name(client=client)
    spark_cluster_version = SparkDiscovery.get(discovery.spark_cluster_version(), client=client)
    spark_cluster_conf = discovery.conf()
    master_wrapper_url = SparkDiscovery.get(discovery.master_wrapper(), client=client)
    conf = {
        "spark.hadoop.yt.proxy": yt_proxy,
        "spark.hadoop.yt.user": yt_user,
        "spark.master.rest.enabled": "true",
        "spark.eventLog.dir": "ytEventLog:/{}".format(discovery.event_log_table()),
        "spark.yt.cluster.version": spark_cluster_version,
        "spark.base.discovery.path": discovery.base_discovery_path
    }
    if is_supported_cluster_minor_version(spark_cluster_version, "1.9"):
        conf["spark.master.driverIdRegistration.enabled"] = "true"
    else:
        conf["spark.master.driverIdRegistration.enabled"] = "false"

    if exists(spark_cluster_conf, client=client):
        conf["spark.yt.cluster.confPath"] = str(spark_cluster_conf)
    if master_wrapper_url:
        conf["spark.hadoop.yt.masterWrapper.url"] = master_wrapper_url
    return conf


def set_conf(conf, dict_conf):
    if dict_conf is not None:
        for (key, value) in dict_conf.items():
            conf.set(key, value)


def default_discovery_dir():
    return os.getenv("SPARK_YT_DISCOVERY_DIR") or YPath("//home").join(os.getenv("USER")).join("spark-tmp")


def default_proxy():
    return os.getenv("YT_PROXY")


def default_tvm_secret():
    return os.getenv("SPARK_TVM_SECRET")


def default_tvm_id():
    return os.getenv("SPARK_TVM_ID")


def get_default_arg_parser(**kwargs):
    parser = argparse.ArgumentParser(**kwargs)
    parser.add_argument("--id", required=False)
    parser.add_argument("--discovery-path", required=False)
    parser.add_argument("--discovery-dir", required=False)
    parser.add_argument("--proxy", required=False, default=default_proxy())
    return parser


def parse_args(parser=None, parser_arguments=None):
    parser_arguments = parser_arguments or {}
    parser = parser or get_default_arg_parser(**parser_arguments)
    args, unknown_args = parser.parse_known_args()
    args.discovery_path = args.discovery_path or args.discovery_dir or default_discovery_dir()
    return args, unknown_args

# backward compatibility
def tuple(element_types):
    from .types import tuple_type
    return tuple_type(element_types)

def spark_home():
    spark_home = os.environ.get("SPARK_HOME")
    if spark_home is None:
        try:
            spark_home = subprocess.check_output("find_spark_home.py").strip().decode("utf-8")
        except:
            raise RuntimeError("Unable to find SPARK_HOME automatically from {}".format(os.path.realpath(__file__)))
    return spark_home

