import collections
import hashlib
import json
import subprocess
import pytest


from yt.environment import arcadia_interop


def get_shasum(data):
    return hashlib.sha1(data).hexdigest()


def compression_testcases():
    return json.load(open("compression_testsets/testcases.json"))


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


def test_compression(caching_file_getter):
    for testcase_info in get_testcase_info_list():
        uncompressed_data = caching_file_getter.get_file(testcase_info.uncompressed_file["path"])
        assert get_shasum(uncompressed_data) == testcase_info.uncompressed_file["shasum"]
        compressed_data = caching_file_getter.get_file(testcase_info.compressed_file["path"])
        assert get_shasum(compressed_data) == testcase_info.compressed_file["shasum"]

        process = subprocess.Popen(
            [arcadia_interop.search_binary_path("run_codec"), "decompress", testcase_info.compressed_file["codec"]],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
        stdout, stderr = process.communicate(compressed_data)
        assert process.returncode == 0, stderr
        assert stdout == uncompressed_data
