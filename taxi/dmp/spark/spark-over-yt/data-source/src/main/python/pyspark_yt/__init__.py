import yaml
from pyspark import SparkConf
from pyspark.sql import SparkSession

from yt_spark_client.utils import *
from contextlib import contextmanager


@contextmanager
def spark_session(conf=SparkConf()):
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    try:
        yield spark
    finally:
        spark.stop()
        spark._jvm.ru.yandex.spark.yt.fs.YtClientProvider.close()


def create_spark_session(spark_id, yt_cluster=None, config_path=None, spark_conf=None):
    if config_path is not None:
        with open(config_path) as f:
            config = yaml.load(f)
    else:
        config = {}

    yt_proxy = yt_cluster or config.get("yt_proxy", os.getenv("YT_PROXY"))
    yt_user = config.get("yt_user") or os.getenv("YT_USER") or os.getenv("USER")
    yt_token = config.get("yt_token") or os.getenv("YT_TOKEN") or default_token()
    yt_client = create_yt_client(yt_proxy, yt_token)
    discovery_dir = config.get("discovery_dir") or default_discovery_dir(yt_user)
    log_dir = config.get("log_dir") or default_base_log_dir(discovery_dir)
    master = get_spark_master(spark_id, discovery_dir, rest=False, yt_client=yt_client)

    conf = SparkConf()
    set_conf(conf, base_spark_conf(yt_proxy, yt_user, log_dir))
    set_conf(conf, default_dynamic_allocation_conf())
    set_conf(conf, spark_conf)

    return SparkSession.builder.config(conf=conf).master(master).getOrCreate()
