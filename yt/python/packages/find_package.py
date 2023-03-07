#!/usr/bin/python

from __future__ import print_function

import sys
import re

PACKAGE_NAME = sys.argv[1]

PACKAGE_PATTERN = re.compile(r"Package: (.*)")
VERSION_PATTERN = re.compile(r"Version: (.*)")

package = None
version = None

for line in sys.stdin:
    if not line.strip() and package == PACKAGE_NAME:
        print(version)

    package_match = PACKAGE_PATTERN.match(line)
    if package_match:
        package = package_match.group(1)
        continue

    version_match = VERSION_PATTERN.match(line)
    if version_match:
        version = version_match.group(1)
        continue
