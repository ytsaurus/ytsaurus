import logging
import unittest
import yatest.common


logger = logging.getLogger(__name__)


def assert_items_equal(result, expected):
    case = unittest.TestCase()
    case.maxDiff = None
    case.assertCountEqual(result, expected)


def upload_job_file(yt_client, source_path, remote_path):
    logger.debug(f"Uploading {source_path} to {remote_path}")
    yt_client.create("file", remote_path)
    with open(yatest.common.source_path(source_path), 'rb') as file:
        yt_client.write_file(remote_path, file)


def get_python_path():
    # This command will be used for running python tasks on workers
    return yatest.common.get_param('python_path', default="python3.11")


def get_java_home():
    return yatest.common.runtime.global_resources()['JDK11_RESOURCE_GLOBAL']
