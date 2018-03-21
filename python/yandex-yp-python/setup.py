from helpers import get_version

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
    requires = ["yandex-yt >= 0.8.25-0", "yandex-yt-yson-bindings >= 0.3.11-3", "protobuf >= 3.2.1", "grpcio == 1.2.0rc1"]

    version = get_version()

    binaries = ["yp/bin/yp", "yp/bin/yp-local"]

    data_files = []
    scripts = [binary + str(sys.version_info[0]) for binary in binaries]

    if "DEB" not in os.environ:
        scripts.extend(binaries)

    find_packages("yp/packages")
    find_packages("proto")
    setup(
        name = "yandex-yp",
        version = version,
        packages = ["yp"] + recursive("yp/packages") + recursive("proto"),
        scripts = scripts,

        install_requires = requires,

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "Python library for YP.",
        keywords = "yp python yson",

        long_description = "TODO",

        data_files = data_files
    )

if __name__ == "__main__":
    main()
