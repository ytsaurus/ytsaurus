import pytest

from yt.wrapper.client import Yt
import yt.logger as logger

import os
import subprocess
import sys
import tempfile

class TestCompression(object):
    codecs = [
        "snappy",
        "lz4",
        "lz4_high_compression",
        "quick_lz",
        "brotli_1",
        "brotli_2",
        "brotli_3",
        "brotli_4",
        "brotli_5",
        "brotli_6",
        "brotli_7",
        "brotli_8",
        "brotli_9",
        "brotli_10",
        "brotli_11",
        "zlib_1",
        "zlib_2",
        "zlib_3",
        "zlib_4",
        "zlib_5",
        "zlib_6",
        "zlib_7",
        "zlib_8",
        "zlib_9",
        "zstd_1",
        "zstd_2",
        "zstd_3",
        "zstd_4",
        "zstd_5",
        "zstd_6",
        "zstd_7",
        "zstd_8",
        "zstd_9",
        "zstd_10",
        "zstd_11",
        "zstd_12",
        "zstd_13",
        "zstd_14",
        "zstd_15",
        "zstd_16",
        "zstd_17",
        "zstd_18",
        "zstd_19",
        "zstd_20",
        "zstd_21",
        "zstd_legacy",
    ]

    testsets = ["YT-2072", "YT-5096"]

    proxy = "locke.yt.yandex.net"
    test_data_path = "//home/files/test_data/compression"

    def prepare(self):
        self.client = Yt(proxy=self.proxy)

    def test_compression(self):
        self.prepare()
        for testset in self.testsets:
            print >>sys.stderr, 'Using testset "{0}"'.format(testset)

            testset_path = os.path.join(self.test_data_path, testset)
            file_path = os.path.join(testset_path, "data")
            try:
                data = self.client.read_file(file_path).read()
            except Exception as e:
                logger.exception("Unable to download testset %s", file_path)
                return

            for codec in self.codecs:
                codec_data = self.run_codec(codec, testset)
                if codec_data is not None:
                    assert codec_data == data

    def run_codec(self, codec, testset):
        testset_path = os.path.join(self.test_data_path, testset)
        file_path = "{0}/data.compressed.{1}".format(testset_path, codec)
        try:
            data = self.client.read_file(file_path).read()
        except Exception as e:
            logger.info("Unable to download compressed data %s", file_path)
            return None

        process = subprocess.Popen(
            ["run_codec", "decompress", codec],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
        stdout, stderr = process.communicate(data)
        return stdout

