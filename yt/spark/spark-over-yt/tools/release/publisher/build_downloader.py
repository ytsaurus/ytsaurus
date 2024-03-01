import argparse
from os.path import join

from .local_manager import Versions, get_release_level, load_versions, ReleaseLevel
from .remote_manager import ClientBuilder, Client, spark_remote_dir, conf_remote_dir, spyt_remote_dir
from .utils import configure_logger

logger = configure_logger("Build downloader")


def download_spark_fork(downloader: Client, versions: Versions, sources_path: str):
    logger.info("Downloading spark fork files")
    spark_tgz = join(sources_path, 'spark.tgz')
    downloader.read_file(f"{spark_remote_dir(versions)}/spark.tgz", spark_tgz)


def download_spyt(downloader: Client, versions: Versions, sources_path: str):
    logger.info("Downloading SPYT files")
    conf_local_dir = join(sources_path, 'conf')
    spark_launch_conf_file = join(conf_local_dir, 'spark-launch-conf')
    downloader.read_document(f"{conf_remote_dir(versions)}/spark-launch-conf", spark_launch_conf_file)
    sidecar_configs_dir = join(conf_local_dir, 'sidecar_configs')
    for config_name in downloader.list_dir(conf_remote_dir(versions)):
        sidecar_config_file = join(sidecar_configs_dir, config_name)
        downloader.read_file(f"{conf_remote_dir(versions)}/{config_name}", sidecar_config_file)
    if not versions.spyt_version.is_snapshot:
        global_conf_file = join(conf_local_dir, 'global')
        downloader.read_document("conf/global", global_conf_file)
    spyt_package_zip = join(sources_path, 'spyt-package.zip')
    downloader.read_file(f"{spyt_remote_dir(versions)}/spyt-package.zip", spyt_package_zip)


def main(sources_path: str, downloader_builder: ClientBuilder):
    release_level = get_release_level(sources_path)
    versions = load_versions(sources_path)
    downloader = Client(downloader_builder)
    if release_level < ReleaseLevel.SPARK_FORK:
        download_spark_fork(downloader, versions, sources_path)
    if release_level < ReleaseLevel.SPYT:
        download_spyt(downloader, versions, sources_path)
    logger.info("Downloaded successfully")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="SPYT publisher")
    parser.add_argument("sources", type=str, help="Path to SPYT sources")
    parser.add_argument("--root", default="//home/spark", type=str, help="Root spyt path on YTsaurus cluster")
    args, _ = parser.parse_known_args()

    downloader_builder = ClientBuilder(
        root_path=args.root,
    )

    main(sources_path=args.sources, downloader_builder=downloader_builder)
