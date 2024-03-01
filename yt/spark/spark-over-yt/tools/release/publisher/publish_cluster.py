#!/usr/bin/env python
import argparse
from os import listdir
from os.path import join

from .local_manager import Versions, get_release_level, load_versions, ReleaseLevel
from .remote_manager import ClientBuilder, conf_remote_dir, spark_remote_dir, PublishConfig, \
    spyt_remote_dir, Client
from .utils import configure_logger

logger = configure_logger("Cluster publisher")


def upload_livy(uploader: Client, sources_path: str):
    logger.info("Uploading livy files")
    uploader.mkdir("livy")
    livy_tgz = join(sources_path, 'livy.tgz')
    if not uploader.exists("livy/livy.tgz"):
        uploader.write_file(livy_tgz, "livy/livy.tgz")


def upload_spark_fork(uploader: Client, versions: Versions, sources_path: str, publish_conf: PublishConfig):
    logger.info("Uploading spark fork files")
    ttl = publish_conf.snapshot_ttl if versions.spark_version.is_snapshot else None
    uploader.mkdir(spark_remote_dir(versions), ttl=ttl, ignore_existing=publish_conf.ignore_existing)

    spark_tgz = join(sources_path, 'spark.tgz')
    uploader.write_file(spark_tgz, f"{spark_remote_dir(versions)}/spark.tgz")


def upload_spyt(uploader: Client, versions: Versions, sources_path: str, publish_conf: PublishConfig):
    logger.info("Uploading SPYT files")
    ttl = publish_conf.snapshot_ttl if versions.spyt_version.is_snapshot else None
    uploader.mkdir(spyt_remote_dir(versions), ttl=ttl, ignore_existing=publish_conf.ignore_existing)
    uploader.mkdir(conf_remote_dir(versions), ttl=ttl, ignore_existing=publish_conf.ignore_existing)

    spyt_package_zip = join(sources_path, 'spyt-package.zip')
    uploader.write_file(spyt_package_zip, f"{spyt_remote_dir(versions)}/spyt-package.zip")

    setup_spyt_env = join(sources_path, 'setup-spyt-env.sh')
    uploader.write_file(setup_spyt_env, f"{spyt_remote_dir(versions)}/setup-spyt-env.sh", executable=True)

    uploader.link(f"{spark_remote_dir(versions)}/spark.tgz", f"{spyt_remote_dir(versions)}/spark.tgz")

    conf_local_dir = join(sources_path, 'conf')
    spark_launch_conf_file = join(conf_local_dir, 'spark-launch-conf')
    uploader.write_document(spark_launch_conf_file, f"{conf_remote_dir(versions)}/spark-launch-conf")
    sidecar_configs_dir = join(conf_local_dir, 'sidecar_configs')
    for config_name in listdir(sidecar_configs_dir):
        sidecar_config_file = join(sidecar_configs_dir, config_name)
        uploader.write_file(sidecar_config_file, f"{conf_remote_dir(versions)}/{config_name}")
    if not versions.spyt_version.is_snapshot:
        global_conf_file_name = publish_conf.specific_global_file or 'global'
        global_conf_file = join(conf_local_dir, global_conf_file_name)
        uploader.write_document(global_conf_file, "conf/global")


def create_base_dirs(uploader: Client, versions: Versions):
    logger.debug("Creation root directories")
    uploader.mkdir(f"conf/{versions.spyt_version.get_release_mode()}")
    uploader.mkdir(f"spark/{versions.spark_version.get_release_mode()}")
    uploader.mkdir(f"spyt/{versions.spyt_version.get_release_mode()}")


def main(sources_path: str, uploader_builder: ClientBuilder, publish_conf: PublishConfig):
    release_level = get_release_level(sources_path)
    versions = load_versions(sources_path)
    uploader = Client(uploader_builder)
    create_base_dirs(uploader, versions)
    if publish_conf.include_livy:
        upload_livy(uploader, sources_path)
    if release_level >= ReleaseLevel.SPARK_FORK and not publish_conf.skip_spark_fork:
        upload_spark_fork(uploader, versions, sources_path, publish_conf)
    if release_level >= ReleaseLevel.SPYT:
        upload_spyt(uploader, versions, sources_path, publish_conf)
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
    parser.add_argument('--include-livy', action='store_true', dest='include_livy', help='Include built Livy')
    parser.set_defaults(include_livy=True)
    args, _ = parser.parse_known_args()

    publish_conf = PublishConfig(
        skip_spark_fork=args.skip_spark_fork,
        specific_global_file=args.specific_global_file,
        ignore_existing=args.ignore_existing,
        include_livy=args.include_livy,
    )
    uploader_builder = ClientBuilder(
        root_path=args.root,
    )

    main(sources_path=args.sources, uploader_builder=uploader_builder, publish_conf=publish_conf)
