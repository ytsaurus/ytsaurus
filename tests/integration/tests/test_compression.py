import pytest

from compression_testcases import TESTCASE_MAP

from yt.wrapper.client import Yt
from yt.packages.requests.exceptions import ConnectionError

import collections
import hashlib
import os
import subprocess
import sys

def get_shasum(data):
    return hashlib.sha1(data).hexdigest()

def get_testcase_info_list():
    TestCaseInfo = collections.namedtuple("TestCaseInfo", ["testset", "uncompressed_file", "compressed_file"])
    result = []
    for testset_name, testset_info in TESTCASE_MAP["testsets"].iteritems():
        uncompressed_file = testset_info["uncompressed_file"]
        for compressed_file in testset_info["compressed_files"]:
            result.append(TestCaseInfo(testset_name, uncompressed_file, compressed_file))
    return result

@pytest.fixture(scope="module")
def yt_proxy():
    # If we have teamcity yt token use it otherwise use default token.
    token = os.environ.get("TEAMCITY_YT_TOKEN", None)
    yt_proxy = Yt(proxy=TESTCASE_MAP["proxy"], token=token)
    try:
        yt_proxy.exists("/")
        return yt_proxy
    except ConnectionError:
        # If we have network errors we don't return client and tests will be skipped.
        return None

class CachingFileGetter(object):
    def __init__(self, yt_proxy):
        self.yt_proxy = yt_proxy
        self.cache = {}

    def get_file(self, yt_path):
        if yt_path not in self.cache:
            self.cache[yt_path] = self.yt_proxy.read_file(yt_path).read()
        return self.cache[yt_path]

@pytest.fixture(scope="module")
def caching_file_getter(yt_proxy):
    return CachingFileGetter(yt_proxy)

@pytest.mark.parametrize("testcase_info", get_testcase_info_list())
def test_compression(yt_proxy, caching_file_getter, testcase_info):
    if yt_proxy is None:
        pytest.skip("yt is unavailable")
    uncompressed_data = caching_file_getter.get_file(testcase_info.uncompressed_file["yt_path"])
    if get_shasum(uncompressed_data) != testcase_info.uncompressed_file["shasum"]:
        print >>sys.stderr, "LENGTH OF UNCOMPRESSED DATA", len(uncompressed_data)
        with open("/home/teamcity/failed_compression_test_file") as fout:
            fout.write(uncompressed_data)
    assert get_shasum(uncompressed_data) == testcase_info.uncompressed_file["shasum"]
    compressed_data = yt_proxy.read_file(testcase_info.compressed_file["yt_path"]).read()
    assert get_shasum(compressed_data) == testcase_info.compressed_file["shasum"]

    process = subprocess.Popen(
        ["run_codec", "decompress", testcase_info.compressed_file["codec"]],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE)
    stdout, stderr = process.communicate(compressed_data)
    assert process.returncode == 0, stderr
    assert stdout == uncompressed_data
