import logging
import os
from tempfile import TemporaryDirectory
from typing import Optional

from spyt.dependency_utils import require_yt_client
require_yt_client()

from yt.wrapper.common import is_arcadia_python  # noqa: E402


logger = logging.getLogger(__name__)

extracted_spyt_dir: Optional[TemporaryDirectory] = None
extracted_spark_dir: Optional[TemporaryDirectory] = None


def _make_executables(directory):
    logger.debug(f"Making executables files in {directory}")
    for name in os.listdir(directory):
        os.chmod(os.path.join(directory, name), 0o755)


def _extract_resources(resource_key_prefix: str, remove_prefix: str, destination_dir: str):
    from library.python import resource
    if len(remove_prefix) > 0 and remove_prefix[-1] != '/':
        logger.warn("Last symbol in remove_prefix is not '/', you may try unpack to machine's root")
    logger.debug(f"Finding resource files with prefix {resource_key_prefix}")
    resource_paths = resource.resfs_files(resource_key_prefix)
    logger.debug(f"Found {len(resource_paths)} files. Extracting...")
    for resource_path in resource_paths:
        assert resource_path.startswith(remove_prefix)
        relative_resource_path = resource_path[len(remove_prefix):]
        destination_path = os.path.join(destination_dir, relative_resource_path)
        os.makedirs(os.path.dirname(destination_path), exist_ok=True)
        with open(destination_path, 'bw') as out:
            out.write(resource.resfs_read(resource_path))


def _extract_spark():
    temp_dir = TemporaryDirectory()
    logger.info(f"Created Spark temp dir {temp_dir}")
    pyspark_dir = "contrib/python/ytsaurus-pyspark/pyspark/"
    pyspark_subdirs = [f"{pyspark_dir}{subdir}" for subdir in ["bin", "conf", "jars"]]
    spyt_spark_extra_dir = "yt/spark/spark-over-yt/spyt-package/src/main/spark-extra/"
    for pyspark_subdir in pyspark_subdirs:
        _extract_resources(pyspark_subdir, pyspark_dir, temp_dir.name)
    _extract_resources(spyt_spark_extra_dir, spyt_spark_extra_dir, temp_dir.name)
    _make_executables(os.path.join(temp_dir.name, "bin"))
    logger.info("Spark files extracted successfully")
    return temp_dir


def _extract_spyt():
    temp_dir = TemporaryDirectory()
    logger.info(f"Created Spyt temp dir {temp_dir}")
    spyt_original_dir = "yt/spark/spark-over-yt/spyt-package/src/main/spyt/"
    _extract_resources(spyt_original_dir, spyt_original_dir, temp_dir.name)
    logger.info("Spyt files extracted successfully")
    return temp_dir


def checked_extract_spyt() -> Optional[str]:
    if not is_arcadia_python():
        return None

    global extracted_spyt_dir
    logger.debug("Arcadia python found")
    if not extracted_spyt_dir:
        extracted_spyt_dir = _extract_spyt()
    logger.debug(f"Current extracted Spyt location {extracted_spyt_dir}")
    return extracted_spyt_dir.name


def checked_extract_spark() -> Optional[str]:
    if not is_arcadia_python():
        return None

    global extracted_spark_dir
    logger.debug("Arcadia python found")
    if not extracted_spark_dir:
        extracted_spark_dir = _extract_spark()
        os.environ["SPARK_HOME"] = extracted_spark_dir.name
    logger.debug(f"Current extracted Spark location {extracted_spark_dir}")
    return extracted_spark_dir.name
