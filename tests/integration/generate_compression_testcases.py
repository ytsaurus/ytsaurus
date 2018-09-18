#!/usr/bin/env python
# -*- coding: utf-8 -*-

import yt.wrapper

import argparse
import collections
import datetime
import hashlib
import logging
import os
import pprint
import subprocess
import sys
import tempfile

# User must be entertained!
logging.basicConfig(
        format="%(asctime)-15s %(levelname)s: %(message)s",
        level=logging.INFO)

InputFileInfo = collections.namedtuple("InputFileInfo", ["yt_path", "testset", "shasum"])

GENERATION = 0
PROXY = "locke.yt.yandex.net"
THIS_FILE_DIRECTORY = os.path.dirname(os.path.abspath(__file__))

USAGE = """
When added new compression codecs please modify this script:
  1) increment `GENERATION' variable;
  2) modify `get_codec_list' to return new codecs.
Rerun this script (run_codec program MUST be in the $PATH).
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

def get_uncompressed_yt_path(map_node, testset):
    return map_node + "/{0}/data".format(testset)

def get_compressed_yt_path(map_node, testset, codec):
    return map_node + "/{0}/data.compressed.{1}".format(testset, codec)

def get_shasum(data):
    return hashlib.sha1(data).hexdigest()

def get_testcases_file():
    return os.path.join(THIS_FILE_DIRECTORY, "tests", "compression_testcases.py")

def compress_upload_shasum_file(file, codec, destination_yt_path):
    file.seek(0)
    compressed = subprocess.check_output(["run_codec", "compress", codec], stdin=file)

    if not yt.wrapper.exists(destination_yt_path):
        yt.wrapper.create("file", destination_yt_path, recursive=True)
    yt.wrapper.write_file(destination_yt_path, compressed)

    return get_shasum(compressed)

def upload_testset(input_file_info, destination_directory):
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

        uncompressed_yt_path = get_uncompressed_yt_path(destination_directory, input_file_info.testset)
        yt.wrapper.copy(input_file_info.yt_path, uncompressed_yt_path, recursive=True)

        testset_info["uncompressed_file"] = {
            "yt_path": uncompressed_yt_path,
            "shasum": shasum,
        }
        compressed_file_infos = []
        testset_info["compressed_files"] = compressed_file_infos
        for codec in get_codec_list():
            compressed_file_yt_path = destination_directory + "/{0}/data.compressed.{1}".format(input_file_info.testset, codec)
            logging.info("Testset: {0} codec: {1} uploading to: {2}".format(input_file_info.testset, codec, compressed_file_yt_path))
            shasum = compress_upload_shasum_file(temp_input_file, codec, compressed_file_yt_path)
            compressed_file_infos.append({
                "yt_path": compressed_file_yt_path,
                "codec": codec,
                "shasum": shasum,
            })
    return testset_info

def exit_with_error(error):
    print >>sys.stderr, "ERROR:"
    print >>sys.stderr, error
    exit(1)

def main():
    argument_parser = argparse.ArgumentParser(description=USAGE, formatter_class=argparse.RawDescriptionHelpFormatter)
    argument_parser.add_argument("--destination-directory",
                                 default="//home/files/test_data/compression/testdata-generation-{0}".format(GENERATION),
                                 help="directory where test data will be saved; by default use current time based name in temporary directory")
    args = argument_parser.parse_args()

    yt.wrapper.config.set_proxy(PROXY)

    destination_directory = args.destination_directory

    try:
        subprocess.check_call("which run_codec", shell=True)
    except subprocess.CalledProcessError:
        exit_with_error("Can't find `run_codec' program in the PATH.")

    if yt.wrapper.exists(destination_directory):
        creation_time_str = yt.wrapper.get("{0}/@creation_time".format(destination_directory))
        creation_time = yt.common.date_string_to_datetime(creation_time_str)
        if datetime.datetime.utcnow() - creation_time > datetime.timedelta(days=1):
            exit_with_error(
                "Trying to modify directory that is quite old.\n"
                "It's probable that this testset is used in current production tests.\n"
                "If you are sure you want to regenerate this test case please remove map_node and restart script.\n"
                "\n"
                "map_node: {0}\n"
                "creation_time: {1}\n".format(destination_directory, creation_time_str))
        else:
            yt.wrapper.remove(destination_directory, recursive=True)

    result_info = {
        "proxy": PROXY,
        "testsets": {},
    }
    for input_file_info in get_input_file_info_list():
        testset = input_file_info.testset
        if testset in result_info["testsets"]:
            raise RuntimeError, "duplicating testset: {0}".format(testset)
        result_info["testsets"][testset] = upload_testset(input_file_info, destination_directory)
    pretty_result_info = pprint.pformat(result_info)
    with open(get_testcases_file(), "w") as testcases_file:
        print >>testcases_file, "# DO NOT EDIT"
        print >>testcases_file, "# THIS IS AUTOMATICALLY GENERATED FILE"
        print >>testcases_file, "#"
        print >>testcases_file, "# generated by {0}".format(__file__)
        print >>testcases_file, ""
        print >>testcases_file, ""
        print >>testcases_file, "TESTCASE_MAP = {0}".format(pretty_result_info)

if __name__ == "__main__":
    main()
