import argparse
import copy
import json
import os
from os import listdir
from os.path import join
import shutil
from typing import List, Dict, Any

from local_manager import get_release_level, load_versions, Versions, ReleaseLevel
from remote_manager import bin_remote_dir, conf_remote_dir, ClientBuilder, Client
from utils import configure_logger


logger = configure_logger("Config generator")


def write_config(config: Dict, conf_file: str):
    logger.debug(f"Writing {conf_file}")
    with open(conf_file, 'w') as file:
        json.dump(config, file, indent=4)


YTSERVER_PROXY = "//sys/bin/ytserver-proxy/ytserver-proxy"

LAUNCH_CONFIG = {
    'spark_conf': {"spark.yt.jarCaching": "true"},
    'environment': {}
}
GLOBAL_CONFIG = {
    'environment': {
        "JAVA_HOME": "/opt/jdk11",
        "IS_SPARK_CLUSTER": "true",
        "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB": "1",
        "ARROW_ENABLE_NULL_CHECK_FOR_GET": "false",
        "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS": "true",
        "SOLOMON_PUSH_PORT": "27099"
    },
    'operation_spec': {
        "job_cpu_monitor": {"enable_cpu_reclaim": "false"}
    },
    'worker_num_limit': 1000
}

SPARK_CONFS = {
    'hahn': {
        "spark.yt.log.enabled": "false",
        "spark.hadoop.yt.proxyRole": "spark",
        "spark.datasource.yt.recursiveFileLookup": "true"
    },
    'hume': {
        "spark.test": "test",
        "spark.datasource.yt.recursiveFileLookup": "true"
    },
    'vanga': {
        "spark.yt.log.enabled": "false",
        "spark.datasource.yt.recursiveFileLookup": "true"
    },
    'arnold': {
        "spark.yt.log.enabled": "false",
        "spark.hadoop.yt.proxyRole": "spark",
        "spark.datasource.yt.recursiveFileLookup": "true"
    }
}


def get_sidecar_configs_dir(conf_local_dir: str):
    return join(conf_local_dir, 'sidecar_configs')


def get_spark_conf(proxy: str):
    proxy_short = proxy.split(".")[0]
    if proxy_short in SPARK_CONFS:
        return SPARK_CONFS[proxy_short]
    else:
        logger.debug("Using default spark conf")
        return {
            "spark.yt.log.enabled": "false",
            "spark.hadoop.yt.proxyRole": "spark",
            "spark.datasource.yt.recursiveFileLookup": "true"
        }


def get_file_paths(conf_local_dir: str, root_path: str, versions: Versions) -> List[str]:
    file_paths = [
        f"{root_path}/{bin_remote_dir(versions)}/spark.tgz",
        f"{root_path}/{bin_remote_dir(versions)}/spark-yt-launcher.jar"
    ]
    file_paths.extend([
        f"{root_path}/{conf_remote_dir(versions)}/{config_name}"
        for config_name in listdir(get_sidecar_configs_dir(conf_local_dir))
    ])
    return file_paths


def prepare_sidecar_configs(conf_local_dir: str, os_release: bool):
    sidecar_configs_dir = get_sidecar_configs_dir(conf_local_dir)
    if os.path.isdir(sidecar_configs_dir):
        logger.info(f"Sidecar configs are already prepared")
    else:
        raw_sidecar_configs_dir = join(conf_local_dir, 'sidecar-config' if os_release else 'inner-sidecar-config')
        shutil.copytree(raw_sidecar_configs_dir, sidecar_configs_dir)
        logger.info(f"Sidecar configs have been copied from {raw_sidecar_configs_dir}")


def prepare_launch_config(conf_local_dir: str, client: Client, versions: Versions,
                          os_release: bool) -> Dict[str, Any]:
    launch_config = copy.deepcopy(LAUNCH_CONFIG)
    launch_config['spark_yt_base_path'] = client.resolve_from_root(bin_remote_dir(versions))
    launch_config['file_paths'] = get_file_paths(conf_local_dir, client_builder.root_path, versions)
    launch_config['enablers'] = {
        "enable_byop": not os_release,
        "enable_preference_ipv6": not os_release,
        "enable_arrow": True,
        "enable_solomon_agent": not os_release,
        "enable_mtn": not os_release,
        "enable_tcp_proxy": os_release
    }
    if not os_release:
        launch_config['layer_paths'] = [
            client.resolve_from_root("delta/layer_with_solomon_agent.tar.gz"),
            "//porto_layers/delta/jdk/layer_with_jdk_lastest.tar.gz",
            client.resolve_from_root("delta/python/layer_with_python311_libs.tar.gz"),
            client.resolve_from_root("delta/python/layer_with_python39_focal.tar.gz"),
            client.resolve_from_root("delta/python/layer_with_python38_libs.tar.gz"),
            client.resolve_from_root("delta/python/layer_with_python37_focal_2.tar.gz"),
            "//porto_layers/base/focal/porto_layer_search_ubuntu_focal_app_lastest.tar.gz"
        ]
        ytserver_proxy_path = client.yt_client.get(f"{YTSERVER_PROXY}&/@target_path")
        logger.info(f"Resolved proxy path: {ytserver_proxy_path}")
        launch_config['ytserver_proxy_path'] = ytserver_proxy_path
    else:
        launch_config['layer_paths'] = []
    return launch_config


def prepare_global_config(versions: Versions, os_release: bool) -> Dict[str, Any]:
    global_config = copy.deepcopy(GLOBAL_CONFIG)
    proxy = os.environ.get("YT_PROXY", "os")
    global_config['spark_conf'] = get_spark_conf(proxy)
    global_config['latest_spark_cluster_version'] = versions.cluster_version.get_scala_version()
    if not os_release:
        python_cluster_paths = {
            "3.11": "/opt/python3.11/bin/python3.11",
            "3.9": "/opt/python3.9/bin/python3.9",
            "3.8": "/opt/python3.8/bin/python3.8",
            "3.7": "/opt/python3.7/bin/python3.7",
            "3.5": "python3.5",
            "3.4": "/opt/python3.4/bin/python3.4",
            "2.7": "python2.7"
        }
        global_config['ytserver_proxy_path'] = YTSERVER_PROXY
    else:
        python_cluster_paths = {
            "3.7": "/opt/conda/bin/python3.7"
        }
    global_config['python_cluster_paths'] = python_cluster_paths
    return global_config


def make_configs(sources_path: str, client_builder: ClientBuilder, versions: Versions, os_release: bool):
    client = Client(client_builder)

    conf_local_dir = join(sources_path, 'conf')

    logger.debug("Sidecar configs preparation")
    prepare_sidecar_configs(conf_local_dir, os_release)
    logger.debug("Launch config file creation")
    launch_config = prepare_launch_config(conf_local_dir, client, versions, os_release)
    logger.info(f"Launch config: {launch_config}")
    write_config(launch_config, join(conf_local_dir, 'spark-launch-conf'))

    if not versions.cluster_version.is_snapshot:
        logger.debug("Global config file creation")
        global_config = prepare_global_config(versions, os_release)
        logger.info(f"Global config: {global_config}")
        write_config(global_config, join(conf_local_dir, 'global'))


def main(sources_path: str, client_builder: ClientBuilder, os_release: bool):
    release_level = get_release_level(sources_path)
    if release_level < ReleaseLevel.CLUSTER:
        raise RuntimeError(f"Found no cluster files")
    versions = load_versions(sources_path)
    make_configs(sources_path, client_builder, versions, os_release)
    logger.info("Generation finished successfully")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SPYT config maker")
    parser.add_argument("sources", type=str, help="Path to SPYT sources")
    parser.add_argument("--root", default="//home/spark", type=str, help="Root spyt path on YTsaurus cluster")
    parser.add_argument("--inner-release", action='store_false', dest='os_release', help="Includes extra settings")
    parser.set_defaults(os_release=True)
    args = parser.parse_args()

    client_builder = ClientBuilder(
        root_path=args.root,
    )

    main(sources_path=args.sources, client_builder=client_builder, os_release=args.os_release)
