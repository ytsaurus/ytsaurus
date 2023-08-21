import logging

from spyt.dependency_utils import require_yt_client
require_yt_client()

from yt.wrapper import get, YPath, list as yt_list, exists
from yt.wrapper.common import update_inplace
from .version import __scala_version__

SPARK_BASE_PATH = YPath("//home/spark")

CONF_BASE_PATH = SPARK_BASE_PATH.join("conf")
GLOBAL_CONF_PATH = CONF_BASE_PATH.join("global")

SPYT_BASE_PATH = SPARK_BASE_PATH.join("spyt")

RELEASES_SUBDIR = "releases"
SNAPSHOTS_SUBDIR = "snapshots"

SELF_VERSION = __scala_version__


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logger = logging.getLogger(__name__)


def validate_cluster_version(spark_cluster_version, client=None):
    if not check_cluster_version_exists(spark_cluster_version, client=client):
        raise RuntimeError("Unknown SPYT cluster version: {}. Available release versions are: {}".format(
            spark_cluster_version, get_available_cluster_versions(client=client)
        ))
    spyt_minor_version = _get_spyt_minor_version(SELF_VERSION)
    spark_cluster_minor_version = _get_spark_cluster_minor_version(spark_cluster_version)
    if spyt_minor_version < spark_cluster_minor_version:
        logger.warning("You required Spark Cluster version {} which is older than spark-launch-yt version {}."
                       "Please update your local ytsaurus-spyt".format(spark_cluster_version, SELF_VERSION))


def is_supported_cluster_minor_version(spark_cluster_version, required_version):
    return get_version_tuple(_get_spark_cluster_minor_version(spark_cluster_version)) >= \
           get_version_tuple(required_version)


def get_version_tuple(version):
    return list(map(int, version.split(".")))


def validate_spyt_version(spyt_version, client=None):
    if not check_spyt_version_exists(spyt_version, client=client):
        raise RuntimeError("Unknown SPYT library version: {}. Available release versions are: {}".format(
            spyt_version, get_available_spyt_versions(client=client)
        ))
    if spyt_version > SELF_VERSION:
        logger.warning("You required SPYT library version {} which is older than your local version {}. "
                       "Some new features may not work as expected. "
                       "Please update your local ytsaurus-spyt".format(spyt_version, SELF_VERSION))


def validate_versions_compatibility(spyt_version, spark_cluster_version):
    spyt_minor_version = _get_spyt_minor_version(spyt_version)
    spark_cluster_minor_version = _get_spark_cluster_minor_version(spark_cluster_version)
    if spyt_minor_version > spark_cluster_minor_version:
        logger.warning("Your SPYT library has version {} which is older than your cluster version {}. "
                       "Some new features may not work as expected. "
                       "Please update your cluster with spark-launch-yt utility".format(spyt_version, spark_cluster_version))


def validate_mtn_config(enablers, network_project, tvm_id, tvm_secret):
    if enablers.enable_mtn and not network_project:
        raise RuntimeError("When using MTN, network_project arg must be set.")


def latest_compatible_spyt_version(spark_cluster_version, client=None):
    spark_cluster_minor_version = _get_spark_cluster_minor_version(spark_cluster_version)
    spyt_versions = get_available_spyt_versions(client)
    compatible_spyt_versions = [x for x in spyt_versions if _get_spyt_minor_version(x) <= spark_cluster_minor_version]
    return max(compatible_spyt_versions)


def latest_cluster_version(global_conf):
    return global_conf["latest_spark_cluster_version"]


def python_bin_path(global_conf, version):
    return global_conf["python_cluster_paths"].get(version)


def worker_num_limit(global_conf):
    return global_conf.get("worker_num_limit", 1000)


def validate_worker_num(worker_num, worker_num_lim):
    if worker_num > worker_num_lim:
        raise RuntimeError("Number of workers ({0}) is more than limit ({1})".format(worker_num, worker_num_lim))


def validate_ssd_config(ssd_limit, ssd_account):
    if ssd_limit is not None and ssd_account is None:
        raise RuntimeError("Ssd account must be provided to use ssd limit, please add --ssd-account option")



def get_available_cluster_versions(client=None):
    subdirs = yt_list(CONF_BASE_PATH.join(RELEASES_SUBDIR), client=client)
    return [x for x in subdirs if x != "spark-launch-conf"]


def check_cluster_version_exists(cluster_version, client=None):
    return exists(_get_version_conf_path(cluster_version), client=client)


def read_global_conf(client=None):
    return client.get(GLOBAL_CONF_PATH)


def read_remote_conf(global_conf, cluster_version, client=None):
    version_conf_path = _get_version_conf_path(cluster_version)
    version_conf = get(version_conf_path, client=client)
    version_conf["cluster_version"] = cluster_version
    return update_inplace(global_conf, version_conf) # TODO(alex-shishkin): Might cause undefined behaviour


def read_cluster_conf(path=None, client=None):
    if path is None:
        return {}
    return get(path, client=client)


def update_config_inplace(base, patch):
    file_paths = _get_or_else(patch, "file_paths", []) + _get_or_else(base, "file_paths", [])
    layer_paths = _get_or_else(patch, "layer_paths", []) + _get_or_else(base, "layer_paths", [])
    update_inplace(base, patch)
    base["file_paths"] = file_paths
    base["layer_paths"] = layer_paths
    return base


def validate_custom_params(params):
    if params and "enablers" in params:
        raise RuntimeError("Argument 'params' contains 'enablers' field, which is prohibited. "
                           "Use argument 'enablers' instead")


def spyt_jar_path(spyt_version):
    return _get_spyt_version_path(spyt_version).join("spark-yt-data-source.jar")


def spyt_python_path(spyt_version):
    return _get_spyt_version_path(spyt_version).join("spyt.zip")


def check_spyt_version_exists(spyt_version, client=None):
    return exists(_get_spyt_version_path(spyt_version), client=client)


def get_available_spyt_versions(client=None):
    return yt_list(SPYT_BASE_PATH.join(RELEASES_SUBDIR), client=client)


def latest_ytserver_proxy_path(cluster_version, client=None):
    if cluster_version:
        return None
    global_conf = read_global_conf(client=client)
    symlink_path = global_conf.get("ytserver_proxy_path")
    if symlink_path is None:
        return None
    return get("{}&/@target_path".format(symlink_path), client=client)


def ytserver_proxy_attributes(path, client=None):
    return get("{}/@user_attributes".format(path), client=client)


def _get_or_else(d, key, default):
    return d.get(key) or default


def _version_subdir(version):
    return SNAPSHOTS_SUBDIR if "SNAPSHOT" in version or "beta" in version or "dev" in version else RELEASES_SUBDIR


def _get_version_conf_path(cluster_version):
    return CONF_BASE_PATH.join(_version_subdir(cluster_version)).join(cluster_version).join("spark-launch-conf")


def _get_spyt_version_path(spyt_version):
    return SPYT_BASE_PATH.join(_version_subdir(spyt_version)).join(spyt_version)


def _get_spyt_minor_version(spyt_version):
    return ".".join(spyt_version.split(".")[:2])


def _get_spark_cluster_minor_version(spark_cluster_version):
    return ".".join(spark_cluster_version.split("-")[0].split(".")[:2])
