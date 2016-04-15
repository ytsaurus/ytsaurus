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
        "zlib6",
        "zlib9",
        "lz4",
        "lz4_high_compression",
        "quick_lz",
        "zstd",
        "brotli3",
        "brotli5",
        "brotli8"]

    testsets = ["data"]

    proxy = "locke.yt.yandex.net"
    test_data_path = "//home/files/test_data/compression"

    def prepare(self):
        self.client = Yt(proxy=self.proxy)

    def test_compression(self):
        self.prepare()
        for testset in self.testsets:
            print >>sys.stderr, 'Using testset "{0}"'.format(testset)

            file_path = "{0}/{1}".format(self.test_data_path, testset)
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
        file_path = "{0}/{1}.compressed.{2}".format(self.test_data_path, testset, codec)
        try:
            data = self.client.read_file(file_path).read()
        except Exception as e:
            logger.exception("Unable to download compressed data %s", file_path)
            return None

        process = subprocess.Popen(
            ["run_codec", "decompress", codec],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE)
        stdout, stderr = process.communicate(data)
        return stdout

