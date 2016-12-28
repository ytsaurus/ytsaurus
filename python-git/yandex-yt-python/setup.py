from helpers import get_version, prepare_files

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import os
import sys
import subprocess

try:
    from itertools import imap
except ImportError:  # Python 3
    imap = map

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ["-vs"]
        self.test_suite = True
        subprocess.check_call("cd yt/wrapper && make", shell=True)

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        pytest.main(self.test_args)
        subprocess.check_call("cd yt/wrapper && make clean", shell=True)

def recursive(path):
    prefix = path.strip("/").replace("/", ".")
    return list(imap(lambda package: prefix + "." + package, find_packages(path))) + [prefix]

def main():
    requires = []
    if sys.version_info[:2] <= (2, 6):
        requires.append("argparse")

    binaries = [
        "yt/wrapper/bin/mapreduce-yt",
        "yt/wrapper/bin/yt",
        "yt/wrapper/bin/yt-fuse",
        "yt/wrapper/bin/yt-admin",
        "yt/wrapper/bin/yt-job-tool"]

    scripts, data_files = prepare_files(binaries, add_major_version_suffix=True)

    if "DEB" in os.environ:
        if sys.version_info[0] < 3:
            data_files += [("/etc/bash_completion.d/", ["yandex-yt-python/yt_completion"])]
    else:
        # NOTE: Binaries are put into EGG-INFO/scripts directory
        # inside egg file and executed with special script wrappers.
        # See https://setuptools.readthedocs.io/en/latest/formats.html#script-wrappers

        # We cannot create link in the egg, so we must put yt binary.
        scripts.extend(binaries)

    version = get_version()
    with open("yt/wrapper/version.py", "w") as version_output:
        version_output.write("VERSION='{0}'".format(version))

    find_packages("yt/packages")
    setup(
        name = "yandex-yt",
        version = version,
        packages = ["yt", "yt.wrapper", "yt.yson"] + recursive("yt/packages"),
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

        # Using py.test, because it much more verbose
        cmdclass = {'test': PyTest},
        tests_require = ['pytest'],

        data_files = data_files
    )

if __name__ == "__main__":
    main()
