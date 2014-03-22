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

def main():
    requires =["simplejson"]
    if sys.version_info[:2] <= (2, 6):
        requires.append("argparse")


    scripts = []
    data_files = []
    # in egg and debian cases strategy of binary distribution is different
    if "EGG" in os.environ:
        scripts.append("yt/wrapper/mapreduce-yt")
        scripts.append("yt/wrapper/yt2")
    else:
        data_files.append(("/usr/bin", ["yt/wrapper/mapreduce-yt"]))
        data_files.append(("/usr/bin", ["yt/wrapper/yt2"]))
    
    version = subprocess.check_output("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True)

    find_packages("yt/packages")
    setup(
        name = "YandexYt",
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
