#!/usr/bin/env python

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "teamcity-build/python"))

from python_packaging.pypi_helpers import get_package_versions

def main():
    package_name, target_version = sys.argv[1:]
    sys.stdout.write(str(int(target_version in get_package_versions(package_name))))

if __name__ == "__main__":
    main()
