from helpers import get_version, prepare_files

from setuptools import setup, find_packages

import os
import sys

try:
    from itertools import imap
except ImportError:  # Python 3
    imap = map

def recursive(path):
    prefix = path.strip("/").replace("/", ".")
    return list(imap(lambda package: prefix + "." + package, find_packages(path))) + [prefix]

def main():
    requires = []
    if sys.version_info[:2] <= (2, 6):
        requires.append("argparse")

    version = get_version()
    with open("yt/wrapper/version.py", "w") as version_output:
        version_output.write("VERSION='{0}'".format(version))

    binaries = [
        "yt/wrapper/bin/mapreduce-yt",
        "yt/wrapper/bin/yt",
        "yt/wrapper/bin/yt-fuse",
        "yt/wrapper/bin/yt-admin",
        "yt/wrapper/bin/yt-job-tool"]

    if sys.version_info[:2] <= (2, 6):
        scripts, data_files = prepare_files(binaries, add_major_version_suffix=True)
    else:
        data_files = []
        scripts = [binary + str(sys.version_info[0]) for binary in binaries]

    data_files.append(("/etc/bash_completion.d/", ["yandex-yt-python/yt_completion"]))
    if "DEB" not in os.environ:
        scripts.extend(binaries)

    find_packages("yt/packages")
    setup(
        name = "yandex-yt",
        version = version,
        packages = ["yt", "yt.wrapper", "yt.yson", "yt.ypath"] + recursive("yt/packages"),
        scripts = scripts,

        install_requires = requires,

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "Python wrapper for YT system and yson parser.",
        keywords = "yt python wrapper mapreduce yson",

        long_description = \
            "It is python library for YT system that works through http api " \
            "and supports most of the features. It provides a lot of default behaviour in case "\
            "of empty tables and absent paths. Also this package provides mapreduce binary "\
            "(based on python library) that is back compatible with Yamr system.",

        data_files = data_files
    )

if __name__ == "__main__":
    main()
