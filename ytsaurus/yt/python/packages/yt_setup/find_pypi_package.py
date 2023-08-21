#!/usr/bin/env python

import sys

from .helpers import get_version, get_version_branch


def find_pypi_package(package_name):
    from . import pypi_helpers

    version = get_version()
    if "-" in version:
        version, build_number = version.split("-", 1)
    else:
        build_number = "0"

    version_branch = get_version_branch(version)

    if version_branch == "stable":
        version = version + "-" + build_number
    elif version_branch == "testing":
        version = version + "rc1"
    elif version_branch == "unstable":
        version = version + "a1"
    else:
        assert False, "Unknown version branch {}".format(version_branch)

    part_count = len(version.replace("-", ".").split("."))
    return version in pypi_helpers.get_package_versions(package_name, version_part_count=part_count)


def main():
    package_name = sys.argv[1]
    sys.stdout.write(str(int(find_pypi_package(package_name))))


if __name__ == "__main__":
    main()
