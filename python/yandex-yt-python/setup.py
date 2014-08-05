from helpers import get_version, prepare_files

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import os
import sys
import subprocess

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
    return map(lambda package: prefix + "." + package, find_packages(path)) + [prefix]

def build_documentation_files(source, target):
    result = []
    for root, dirs, files in os.walk(source, topdown=False):
        dest = os.path.join(target, root[len(source):])
        result.append((dest, [os.path.join(root, file) for file in files]))
    return result

def main():
    requires =["simplejson", "dateutil"]
    if sys.version_info[:2] <= (2, 6):
        requires.append("argparse")


    scripts, data_files = prepare_files(["yt/wrapper/mapreduce-yt", "yt/wrapper/yt2"])
    if "DEB" in os.environ:
        data_files += build_documentation_files("docs/_build/", "/usr/share/doc/yandex-yt-python-docs")
    
    find_packages("yt/packages")
    setup(
        name = "yandex-yt",
        version = get_version(),
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
