import logging

from yt.wrapper import get, YPath, list as yt_list, exists
from yt.wrapper.common import update_inplace

SPARK_BASE_PATH = YPath("//sys/spark")

CONF_BASE_PATH = SPARK_BASE_PATH.join("conf")
GLOBAL_CONF_PATH = CONF_BASE_PATH.join("global")

SPYT_BASE_PATH = SPARK_BASE_PATH.join("spyt")

RELEASES_SUBDIR = "releases"
SNAPSHOTS_SUBDIR = "snapshots"


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


def validate_cluster_version(spark_cluster_version, client=None):
    if not check_cluster_version_exists(spark_cluster_version, client=client):
        raise RuntimeError("Unknown SPYT cluster version: {}. Available release versions are: {}".format(
            spark_cluster_version, get_available_cluster_versions(client=client)
        ))


def validate_spyt_version(spyt_version, client=None):
    if not check_spyt_version_exists(spyt_version, client=client):
        raise RuntimeError("Unknown SPYT library version: {}. Available release versions are: {}".format(
            spyt_version, get_available_spyt_versions(client=client)
        ))


def validate_versions_compatibility(spyt_version, spark_cluster_version):
    spyt_minor_version = _get_spyt_minor_version(spyt_version)
    spark_cluster_minor_version = _get_spark_cluster_minor_version(spark_cluster_version)
    if spyt_minor_version > spark_cluster_minor_version:
        logger.warning("Your SPYT library has version {} which is older than your cluster version {}. "
                       "Some new features may not work as expected. "
                       "Please update your cluster with spark-launch-yt utility".format(spyt_version, spark_cluster_version))


def latest_compatible_spyt_version(spark_cluster_version, client=None):
    spark_cluster_minor_version = _get_spark_cluster_minor_version(spark_cluster_version)
    spyt_versions = get_available_spyt_versions(client)
    compatible_spyt_versions = [x for x in spyt_versions if _get_spyt_minor_version(x) <= spark_cluster_minor_version]
    return max(compatible_spyt_versions)


def latest_cluster_version(client=None):
    return max(get_available_cluster_versions(client=client))


def get_available_cluster_versions(client=None):
    subdirs = yt_list(CONF_BASE_PATH.join(RELEASES_SUBDIR), client=client)
    return [x for x in subdirs if x != "spark-launch-conf"]


def check_cluster_version_exists(cluster_version, client=None):
    return exists(_get_version_conf_path(cluster_version), client=client)


def read_remote_conf(cluster_version=None, client=None):
    global_conf = get(GLOBAL_CONF_PATH, client=client)
    cluster_version = cluster_version or global_conf["latest_spark_cluster_version"]
    version_conf_path = _get_version_conf_path(cluster_version)
    version_conf = get(version_conf_path, client=client)
    version_conf["cluster_version"] = cluster_version
    return update_inplace(global_conf, version_conf)


def update_config_inplace(base, patch):
    file_paths = _get_or_else(patch, "file_paths", []) + _get_or_else(base, "file_paths", [])
    layer_paths = _get_or_else(patch, "layer_paths", []) + _get_or_else(base, "layer_paths", [])
    update_inplace(base, patch)
    base["file_paths"] = file_paths
    base["layer_paths"] = layer_paths
    return base


def spyt_jar_path(spyt_version):
    return _get_spyt_version_path(spyt_version).join("spark-yt-data-source.jar")


def spyt_python_path(spyt_version):
    return _get_spyt_version_path(spyt_version).join("spyt.zip")


def check_spyt_version_exists(spyt_version, client=None):
    return exists(_get_spyt_version_path(spyt_version), client=client)


def get_available_spyt_versions(client=None):
    return yt_list(SPYT_BASE_PATH.join(RELEASES_SUBDIR), client=client)


def _get_or_else(d, key, default):
    return d.get(key) or default

def _version_subdir(version):
    return SNAPSHOTS_SUBDIR if "SNAPSHOT" in version or "beta" in version else RELEASES_SUBDIR


def _get_version_conf_path(cluster_version):
    return CONF_BASE_PATH.join(_version_subdir(cluster_version)).join(cluster_version).join("spark-launch-conf")


def _get_spyt_version_path(spyt_version):
    return SPYT_BASE_PATH.join(_version_subdir(spyt_version)).join(spyt_version)


def _get_spyt_minor_version(spyt_version):
    return ".".join(spyt_version.split(".")[:2])


def _get_spark_cluster_minor_version(spark_cluster_version):
    return ".".join(spark_cluster_version.split("-")[1].split(".")[:2])
