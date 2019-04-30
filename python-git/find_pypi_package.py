#!/usr/bin/env python

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "teamcity-build/python/python_packaging"))

from helpers import get_version

from pypi_helpers import get_package_versions

def main():
    package_name = sys.argv[1]

    version = get_version()
    version, build_number = version.split("-", 1)

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

    sys.stdout.write(str(int(version in get_package_versions(package_name, version_part_count=part_count))))

if __name__ == "__main__":
    main()
