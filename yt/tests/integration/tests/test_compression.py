from yt_commands import authors, print_debug

from yt_helpers import from_sandbox

import collections
import hashlib
import os
import subprocess
import pytest
import imp

from flaky import flaky

def get_shasum(data):
    return hashlib.sha1(data).hexdigest()

def sandbox_testsets_resource_id():
    return "1144852151"

@pytest.fixture(scope="module")
def compression_testsets_resource():
    from_sandbox(sandbox_testsets_resource_id())

def compression_testcases():
    module_name = "compression_testcases"
    fp, pathname, description = imp.find_module(module_name, [os.path.join(os.getcwd(), "compression_testsets")])
    imp.load_module(module_name, fp, pathname, description)
    from compression_testcases import TESTCASE_MAP
    return TESTCASE_MAP

def get_testcase_info_list():
    TestCaseInfo = collections.namedtuple("TestCaseInfo", ["testset", "uncompressed_file", "compressed_file"])
    result = []
    testcase_map = compression_testcases()
    for testset_name, testset_info in testcase_map["testsets"].iteritems():
        uncompressed_file = testset_info["uncompressed_file"]
        for compressed_file in testset_info["compressed_files"]:
            result.append(TestCaseInfo(testset_name, uncompressed_file, compressed_file))
    return result

class CachingFileGetter(object):
    def __init__(self):
        self.cache = {}

    def get_file(self, path):
        if path not in self.cache:
            with open(path) as f:
                self.cache[path] = f.read()
        return self.cache[path]

@pytest.fixture(scope="module")
def caching_file_getter():
    return CachingFileGetter()

@authors("ermolovd")
@pytest.mark.usefixtures("compression_testsets_resource")
def test_compression(caching_file_getter):
    for testcase_info in get_testcase_info_list():
        uncompressed_data = caching_file_getter.get_file(testcase_info.uncompressed_file["path"])
        if get_shasum(uncompressed_data) != testcase_info.uncompressed_file["shasum"]:
            print_debug("LENGTH OF UNCOMPRESSED DATA", len(uncompressed_data))
            with open("/home/teamcity/failed_compression_test_file") as fout:
                fout.write(uncompressed_data)
        assert get_shasum(uncompressed_data) == testcase_info.uncompressed_file["shasum"]
        compressed_data = caching_file_getter.get_file(testcase_info.compressed_file["path"])
        assert get_shasum(compressed_data) == testcase_info.compressed_file["shasum"]

        process = subprocess.Popen(
            ["run_codec", "decompress", testcase_info.compressed_file["codec"]],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
        stdout, stderr = process.communicate(compressed_data)
        assert process.returncode == 0, stderr
        assert stdout == uncompressed_data
