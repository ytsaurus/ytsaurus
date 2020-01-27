import os

import yt
from yt.wrapper.cypress_commands import list as yt_list


def get_spark_master(spark_id, discovery_dir, rest, yt_client):
    master_path = "rest" if rest else "address"
    master = yt_list("{0}/instances/{1}/{2}".format(discovery_dir, spark_id, master_path), client=yt_client)[0]
    return "spark://{0}".format(master)


def create_yt_client(yt_proxy, yt_token):
    return yt.wrapper.YtClient(proxy=yt_proxy, token=yt_token)


def default_token():
    with open("{}/.yt/token".format(os.getenv("HOME"))) as f:
        token = f.readline().strip()
    return token


def base_spark_conf(yt_proxy, yt_user, log_dir):
    return {
        "spark.hadoop.yt.proxy": yt_proxy,
        "spark.hadoop.yt.user": yt_user,
        "spark.master.rest.enabled": "true",
        "spark.eventLog.dir": "yt:/{}".format(log_dir),
    }


def default_discovery_dir(yt_user):
    return os.getenv("SPARK_YT_DISCOVERY_DIR") or "//home/{0}/spark-tmp".format(yt_user)


def default_base_log_dir(discovery_dir):
    return os.getenv("SPARK_YT_LOG_DIR") or "{}/logs".format(discovery_dir)


def default_user():
    return os.getenv("YT_USER") or os.getenv("USER")
