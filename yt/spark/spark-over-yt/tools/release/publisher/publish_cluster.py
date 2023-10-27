#!/usr/bin/env python
import argparse
from os import listdir
from os.path import join

from local_manager import Versions, get_release_level, load_versions, ReleaseLevel
from remote_manager import ClientBuilder, bin_remote_dir, conf_remote_dir, spark_remote_dir, PublishConfig, \
    spyt_remote_dir, Client
from utils import configure_logger

logger = configure_logger("Cluster publisher")


def upload_spark_fork(uploader: Client, versions: Versions, sources_path: str, publish_conf: PublishConfig):
    logger.info("Uploading spark fork files")
    ttl = publish_conf.snapshot_ttl if versions.spark_version.is_snapshot else None
    uploader.mkdir(spark_remote_dir(versions), ttl=ttl, ignore_existing=publish_conf.ignore_existing)

    spark_tgz = join(sources_path, 'spark.tgz')
    uploader.write_file(spark_tgz, f"{spark_remote_dir(versions)}/spark.tgz")


def upload_cluster(uploader: Client, versions: Versions, sources_path: str, publish_conf: PublishConfig):
    logger.info("Uploading cluster files")
    ttl = publish_conf.snapshot_ttl if versions.cluster_version.is_snapshot else None
    uploader.mkdir(bin_remote_dir(versions), ttl=ttl, ignore_existing=publish_conf.ignore_existing)
    uploader.mkdir(conf_remote_dir(versions), ttl=ttl, ignore_existing=publish_conf.ignore_existing)

    spark_yt_launcher_jar = join(sources_path, 'spark-yt-launcher.jar')
    uploader.write_file(spark_yt_launcher_jar, f"{bin_remote_dir(versions)}/spark-yt-launcher.jar")

    spark_extra_zip = join(sources_path, 'spark-extra.zip')
    uploader.write_file(spark_extra_zip, f"{bin_remote_dir(versions)}/spark-extra.zip")

    uploader.link(f"{spark_remote_dir(versions)}/spark.tgz", f"{bin_remote_dir(versions)}/spark.tgz")

    conf_local_dir = join(sources_path, 'conf')
    spark_launch_conf_file = join(conf_local_dir, 'spark-launch-conf')
    uploader.write_document(spark_launch_conf_file, f"{conf_remote_dir(versions)}/spark-launch-conf")
    sidecar_configs_dir = join(conf_local_dir, 'sidecar_configs')
    for config_name in listdir(sidecar_configs_dir):
        sidecar_config_file = join(sidecar_configs_dir, config_name)
        uploader.write_file(sidecar_config_file, f"{conf_remote_dir(versions)}/{config_name}")
    if not versions.cluster_version.is_snapshot:
        global_conf_file_name = publish_conf.specific_global_file or 'global'
        global_conf_file = join(conf_local_dir, global_conf_file_name)
        uploader.write_document(global_conf_file, f"conf/global")


def upload_client(uploader: Client, versions: Versions, sources_path: str, publish_conf: PublishConfig):
    logger.info("Uploading client files")
    ttl = publish_conf.snapshot_ttl if versions.client_version.is_snapshot else None
    uploader.mkdir(spyt_remote_dir(versions), ttl=ttl, ignore_existing=publish_conf.ignore_existing)

    spyt_zip = join(sources_path, 'spyt.zip')
    uploader.write_file(spyt_zip, f"{spyt_remote_dir(versions)}/spyt.zip")
    spark_yt_data_source_jar = join(sources_path, 'spark-yt-data-source.jar')
    uploader.write_file(spark_yt_data_source_jar, f"{spyt_remote_dir(versions)}/spark-yt-data-source.jar")


def create_base_dirs(uploader: Client, versions: Versions):
    logger.debug("Creation root directories")
    uploader.mkdir(f"bin/{versions.cluster_version.get_release_mode()}")
    uploader.mkdir(f"conf/{versions.cluster_version.get_release_mode()}")
    uploader.mkdir(f"spark/{versions.spark_version.get_release_mode()}")
    uploader.mkdir(f"spyt/{versions.client_version.get_release_mode()}")


def main(sources_path: str, uploader_builder: ClientBuilder, publish_conf: PublishConfig):
    release_level = get_release_level(sources_path)
    versions = load_versions(sources_path)
    uploader = Client(uploader_builder)
    create_base_dirs(uploader, versions)
    if release_level >= ReleaseLevel.SPARK_FORK and not publish_conf.skip_spark_fork:
        upload_spark_fork(uploader, versions, sources_path, publish_conf)
    if release_level >= ReleaseLevel.CLUSTER:
        upload_cluster(uploader, versions, sources_path, publish_conf)
    if release_level >= ReleaseLevel.CLIENT:
        upload_client(uploader, versions, sources_path, publish_conf)
    logger.info("Publication finished successfully")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SPYT publisher")
    parser.add_argument("sources", type=str, help="Path to SPYT sources")
    parser.add_argument("--root", default="//home/spark", type=str, help="Root spyt path on YTsaurus cluster")
    parser.add_argument('--specific-global-file', type=str, default="", help='Specific global conf file name')
    parser.add_argument('--ignore-existing', action='store_true', dest='ignore_existing', help='Overwrite cluster files')
    parser.set_defaults(ignore_existing=False)
    parser.add_argument('--skip-spark-fork', action='store_true', dest='skip_spark_fork', help='Skip spark fork publication')
    parser.set_defaults(skip_spark_fork=False)
    args = parser.parse_args()

    publish_conf = PublishConfig(
        skip_spark_fork=args.skip_spark_fork,
        specific_global_file=args.specific_global_file,
        ignore_existing=args.ignore_existing,
        snapshot_ttl=14 * 24 * 60 * 60 * 1000
    )
    uploader_builder = ClientBuilder(
        root_path=args.root,
    )

    main(sources_path=args.sources, uploader_builder=uploader_builder, publish_conf=publish_conf)
