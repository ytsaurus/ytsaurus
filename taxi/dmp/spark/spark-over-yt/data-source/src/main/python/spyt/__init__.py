import sys

sys.path.append("/usr/lib/yandex/spark/python")

import yaml
import re
from pyspark import SparkConf
from pyspark.sql import SparkSession

from yt_spark_client.utils import default_token, default_discovery_dir, get_spark_master, set_conf, SparkDiscovery
from contextlib import contextmanager
import os
from yt.wrapper import YtClient, read_file, get
from yt.wrapper.http_helpers import get_token, get_user_name, get_proxy_url


def _parse_memory(memory_str):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_str is None:
        return None
    m = re.match("(\d+)(.*)", memory_str)
    value = int(m.group(1))
    unit = m.group(2).lower().strip()
    if len(unit) <= 1:
        unit = unit + "b"
    return value * units[unit]


def _format_memory(memory_bytes):
    units = {"gb": 1024 * 1024 * 1024, "mb": 1024 * 1024, "kb": 1024, "bb": 1, "b": 1}
    if memory_bytes % units["gb"] == 0:
        return "{}G".format(memory_bytes // units["gb"])
    if memory_bytes % units["mb"] == 0:
        return "{}M".format(memory_bytes // units["mb"])
    if memory_bytes % units["kb"] == 0:
        return "{}K".format(memory_bytes // units["kb"])
    return "{}B".format(memory_bytes)


class Defaults(object):
    LOCAL_CONF_PATH = os.path.join(os.getenv("HOME"), "spyt.yaml") if os.getenv("HOME") else None
    REMOTE_CONF_PATH = "//sys/spark/conf/releases/spark-launch-conf"


@contextmanager
def spark_session(num_executors=None,
                  yt_proxy=None,
                  discovery_path=None,
                  app_name=None,
                  cores_per_executor=None,
                  executor_memory_per_core=None,
                  driver_memory=None,
                  dynamic_allocation=False,
                  spark_conf_args=None,
                  local_conf_path=Defaults.LOCAL_CONF_PATH,
                  remote_conf_path=Defaults.REMOTE_CONF_PATH,
                  spark_id=None,
                  client=None):
    conf = _build_spark_conf(
        num_executors=num_executors,
        yt_proxy=yt_proxy,
        discovery_path=discovery_path,
        app_name=app_name,
        cores_per_executor=cores_per_executor,
        executor_memory_per_core=executor_memory_per_core,
        driver_memory=driver_memory,
        dynamic_allocation=dynamic_allocation,
        spark_conf_args=spark_conf_args,
        local_conf_path=local_conf_path,
        remote_conf_path=remote_conf_path,
        spark_id=spark_id,
        client=client
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
        spark._jvm.ru.yandex.spark.yt.fs.YtClientProvider.close()


def get_spark_discovery(discovery_path, spark_id, conf):
    spark_id = spark_id or conf.get("spark_id")
    discovery_path = discovery_path or conf.get("discovery_path") or conf.get(
        "discovery_dir") or default_discovery_dir()
    return SparkDiscovery(discovery_path=discovery_path, spark_id=spark_id)


def create_yt_client(yt_proxy, conf):
    yt_proxy = yt_proxy or conf.get("yt_proxy", os.getenv("YT_PROXY"))
    yt_token = conf.get("yt_token") or default_token()
    return YtClient(proxy=yt_proxy, token=yt_token)


def _configure_client_mode(spark_conf,
                           discovery_path,
                           local_conf,
                           spark_id,
                           client=None):
    _configure_python()
    discovery = get_spark_discovery(discovery_path, spark_id, local_conf)
    master = get_spark_master(discovery, rest=False, yt_client=client)
    event_log_dir = discovery.event_log()
    yt_proxy = get_proxy_url(required=True, client=client)
    yt_user = get_user_name(client=client)
    spark_conf.set("spark.master", master)
    spark_conf.set("spark.eventLog.dir", event_log_dir)
    spark_conf.set("spark.hadoop.yt.proxy", yt_proxy)
    spark_conf.set("spark.hadoop.yt.user", yt_user)
    spark_conf.set("spark.hadoop.yt.token", get_token(client=client))


def _validate_resources(num_executors, cores_per_executor, executor_memory_per_core):
    if num_executors and (not isinstance(num_executors, int) or num_executors < 1):
        raise AssertionError("Invalid number of executors")

    if cores_per_executor and (not isinstance(cores_per_executor, int) or cores_per_executor < 1):
        raise AssertionError("Invalid number of cores per executor")

    if executor_memory_per_core and executor_memory_per_core < 1:
        raise AssertionError("Invalid amount of memory per core")


def _set_none_safe(conf, key, value):
    if value:
        conf.set(key, value)


def _configure_python():
    python_version = "%d.%d" % sys.version_info[:2]
    if python_version == "3.7":
        os.environ["PYSPARK_PYTHON"] = "/opt/python3.7/bin/python3.7"
    elif python_version == "3.5":
        os.environ["PYSPARK_PYTHON"] = "python3.5"
    elif python_version == "2.7":
        os.environ["PYSPARK_PYTHON"] = "python2.7"
    else:
        raise AssertionError("Python version {} is not supported".format(python_version))


def _read_remote_conf(path, client=None):
    return get(path, client=client)["spark_conf"] if path else None


def _read_local_conf(conf_path):
    if os.path.isfile(conf_path):
        with open(conf_path) as f:
            return yaml.load(f, Loader=yaml.BaseLoader)
    return {}


def _build_spark_conf(num_executors=None,
                      yt_proxy=None,
                      discovery_path=None,
                      app_name=None,
                      cores_per_executor=None,
                      executor_memory_per_core=None,
                      driver_memory=None,
                      dynamic_allocation=None,
                      spark_conf_args=None,
                      local_conf_path=None,
                      remote_conf_path=None,
                      spark_id=None,
                      client=None):
    is_client_mode = os.getenv("IS_SPARK_CLUSTER") is None
    local_conf = {}
    spark_conf = SparkConf()

    if is_client_mode:
        local_conf = _read_local_conf(local_conf_path)
        app_name = app_name or "PySpark for {}".format(os.getenv("USER"))
        client = client or create_yt_client(yt_proxy, local_conf)
        _configure_client_mode(spark_conf, discovery_path, local_conf, spark_id, client)

    remote_dynamic_conf = _read_remote_conf(remote_conf_path, client=client)
    set_conf(spark_conf, remote_dynamic_conf)

    num_executors = num_executors or local_conf.get("num_executors")
    cores_per_executor = cores_per_executor or local_conf.get("cores_per_executor")
    executor_memory_per_core = _parse_memory(executor_memory_per_core or local_conf.get("executor_memory_per_core"))
    _validate_resources(num_executors, cores_per_executor, executor_memory_per_core)
    driver_memory = _parse_memory(driver_memory or local_conf.get("driver_memory"))
    executor_memory = executor_memory_per_core * cores_per_executor if executor_memory_per_core and cores_per_executor else None
    max_cores = num_executors * cores_per_executor if num_executors and cores_per_executor else None

    _set_none_safe(spark_conf, "spark.dynamicAllocation.enabled", dynamic_allocation)
    _set_none_safe(spark_conf, "spark.dynamicAllocation.executorIdleTimeout", "10m")
    _set_none_safe(spark_conf, "spark.cores.max", max_cores)
    _set_none_safe(spark_conf, "spark.dynamicAllocation.maxExecutors", num_executors)
    _set_none_safe(spark_conf, "spark.executor.cores", cores_per_executor)
    _set_none_safe(spark_conf, "spark.executor.memory", _format_memory(executor_memory) if executor_memory else None)
    _set_none_safe(spark_conf, "spark.driver.memory", _format_memory(driver_memory) if driver_memory else None)
    _set_none_safe(spark_conf, "spark.driver.maxResultSize", _format_memory(driver_memory) if driver_memory else None)
    _set_none_safe(spark_conf, "spark.app.name", app_name)
    set_conf(spark_conf, spark_conf_args)

    return spark_conf


def connect(num_executors=5,
            yt_proxy=None,
            discovery_path=None,
            app_name=None,
            cores_per_executor=4,
            executor_memory_per_core="4G",
            driver_memory="1G",
            dynamic_allocation=False,
            spark_conf_args=None,
            local_conf_path=Defaults.LOCAL_CONF_PATH,
            remote_conf_path=Defaults.REMOTE_CONF_PATH,
            spark_id=None,
            client=None):
    conf = _build_spark_conf(
        num_executors=num_executors,
        yt_proxy=yt_proxy,
        discovery_path=discovery_path,
        app_name=app_name,
        cores_per_executor=cores_per_executor,
        executor_memory_per_core=executor_memory_per_core,
        driver_memory=driver_memory,
        dynamic_allocation=dynamic_allocation,
        spark_conf_args=spark_conf_args,
        local_conf_path=local_conf_path,
        remote_conf_path=remote_conf_path,
        spark_id=spark_id,
        client=client
    )

    return SparkSession.builder.config(conf=conf).getOrCreate()
