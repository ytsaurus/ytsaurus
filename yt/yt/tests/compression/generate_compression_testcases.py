#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yt.wrapper

import json
import argparse
import collections
import datetime
import hashlib
import logging
import os
import subprocess
import sys
import tempfile
import shutil

# User must be entertained!
logging.basicConfig(
        format="%(asctime)-15s %(levelname)s: %(message)s",
        level=logging.INFO)

InputFileInfo = collections.namedtuple("InputFileInfo", ["yt_path", "testset", "shasum"])

PROXY = "locke.yt.yandex.net"
THIS_FILE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

USAGE = """
When adding new compression codecs please modify this script:
  1) modify `get_codec_list' to return new codecs;
  2) rerun this script (run_codec program MUST be in the $PATH);
  3) upload resulting directory to Sandbox and update resource id in ya.make file accordingly.
     Don't forget to mark the resource as "Important" so that its ttl is set to inf.
"""

def including_xrange(i, j):
    return xrange(i, j + 1)

def get_codec_list():
    return [
        "snappy",
        "lz4",
        "lz4_high_compression",
        "quick_lz",
        "zstd_legacy",
    ] + [
        "brotli_{0}".format(i) for i in including_xrange(1, 11)
    ] + [
        "bzip2_{0}".format(i) for i in including_xrange(1, 9)
    ] + [
        "lzma_{0}".format(i) for i in including_xrange(0, 9)
    ] + [
        "zlib_{0}".format(i) for i in including_xrange(1, 9)
    ] + [
        "zstd_{0}".format(i) for i in including_xrange(1, 21)
    ]

def get_input_file_info_list():
    return [
        InputFileInfo(
            yt_path="//home/files/test_data/compression/template/YT-2072/data",
            testset="YT-2072",
            shasum="95b75100e74db32c2714bc691861888e5b589322"),
        InputFileInfo(
            yt_path="//home/files/test_data/compression/template/YT-5096/data",
            testset="YT-5096",
            shasum="f49966824a617322ff9c0797656e17d6dda1429a"),
    ]

def get_uncompressed_path(map_node, testset):
    return map_node + "/{0}/data".format(testset)

def get_compressed_path(map_node, testset, codec):
    return map_node + "/{0}/data.compressed.{1}".format(testset, codec)

def get_shasum(data):
    return hashlib.sha1(data).hexdigest()

def get_testcases_file(destination_directory):
    return os.path.join(destination_directory, "testcases.json")

def compress_save_shasum_file(file, codec, destination_path):
    file.seek(0)
    compressed = subprocess.check_output(["run_codec", "compress", codec], stdin=file)

    if not os.path.exists(os.path.dirname(destination_path)):
        os.makedirs(os.path.dirname(destination_path))
    with open(destination_path, "w") as f:
        f.write(compressed)

    return get_shasum(compressed)

def save_testset(input_file_info, destination_directory):
    testset_info = {}
    with tempfile.TemporaryFile() as temp_input_file:
        input_data = yt.wrapper.read_file(input_file_info.yt_path).read()
        shasum = get_shasum(input_data)
        if shasum != input_file_info.shasum:
            exit_with_error(
                "SHA1 sum mismatch\n"
                "Path: {path}\n"
                "Expected: {expected}\n"
                "Actual: {actual}\n".format(
                    path=input_file_info.yt_path,
                    expected=input_file_info.shasum,
                    actual=shasum))
        temp_input_file.write(input_data)
        temp_input_file.flush()

        uncompressed_path = get_uncompressed_path(destination_directory, input_file_info.testset)
        if not os.path.exists(os.path.dirname(uncompressed_path)):
            os.makedirs(os.path.dirname(uncompressed_path))
        with open(uncompressed_path, "w") as f:
            f.write(input_data)

        testset_info["uncompressed_file"] = {
            "path": uncompressed_path,
            "shasum": shasum,
        }
        compressed_file_infos = []
        testset_info["compressed_files"] = compressed_file_infos
        for codec in get_codec_list():
            compressed_file_path = destination_directory + "/{0}/data.compressed.{1}".format(input_file_info.testset, codec)
            logging.info("Testset: {0} codec: {1} saving to: {2}".format(input_file_info.testset, codec, compressed_file_path))
            shasum = compress_save_shasum_file(temp_input_file, codec, compressed_file_path)
            compressed_file_infos.append({
                "path": compressed_file_path,
                "codec": codec,
                "shasum": shasum,
            })
    return testset_info

def exit_with_error(error):
    print("ERROR:", error)
    exit(1)

def main():
    argument_parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    args = argument_parser.parse_args()

    yt.wrapper.config.set_proxy(PROXY)

    destination_directory = "compression_testsets"
    shutil.rmtree(destination_directory, ignore_errors=True)

    try:
        subprocess.check_call("which run_codec", shell=True)
    except subprocess.CalledProcessError:
        exit_with_error("Can't find `run_codec' program in the PATH.")

    result_info = {
        "testsets": {},
    }
    for input_file_info in get_input_file_info_list():
        testset = input_file_info.testset
        if testset in result_info["testsets"]:
            raise RuntimeError, "duplicating testset: {0}".format(testset)
        result_info["testsets"][testset] = save_testset(input_file_info, destination_directory)

    with open(get_testcases_file(destination_directory), "w") as testcases_file:
        json.dump(result_info, testcases_file)

    print("{0} was created. Upload it to Sandbox to use it in tests.".format(destination_directory))

if __name__ == "__main__":
    main()
