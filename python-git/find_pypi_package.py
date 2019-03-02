#!/usr/bin/env python

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "teamcity-build/python/python_packaging"))

from pypi_helpers import get_package_versions

def main():
    package_name, target_version = sys.argv[1:]
    part_count = len(target_version.replace("-", ".").split("."))
    sys.stdout.write(str(int(target_version in get_package_versions(package_name, version_part_count=part_count))))

if __name__ == "__main__":
    main()
