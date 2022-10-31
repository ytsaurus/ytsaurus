#!/usr/bin/env python

import os
import sys

from helpers import get_version, import_file


def find_pypi_package(package_name):
    try:
        import pypi_helpers
    except ImportError:
        pypi_helpers = import_file("pypi_helpers", "./pypi_helpers.py")

    version = get_version()
    if "-" in version:
        version, build_number = version.split("-", 1)
    else:
        build_number = "0"

    is_stable = True
    if os.path.exists("stable_versions"):
        stable_versions = []
        with open("stable_versions") as fin:
            stable_versions = fin.read().split("\n")
        if version not in stable_versions:
            is_stable = False

    if is_stable:
        version = version + "-" + build_number
    else:
        version = version + "a1"

    part_count = len(version.replace("-", ".").split("."))
    return version in pypi_helpers.get_package_versions(package_name, version_part_count=part_count)


def main():
    package_name = sys.argv[1]
    sys.stdout.write(str(int(find_pypi_package(package_name))))


if __name__ == "__main__":
    main()
