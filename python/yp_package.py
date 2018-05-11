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

def setup_package(name, python_dependent_requires):
    requires = ["yandex-yt >= 0.8.25-0", "protobuf >= 3.2.1", "grpcio == 1.2.0rc1"] + python_dependent_requires

    version = get_version()

    binaries = ["yp/bin/yp", "yp/bin/yp-local"]

    data_files = []
    scripts = [binary + str(sys.version_info[0]) for binary in binaries]

    if "DEB" not in os.environ:
        scripts.extend(binaries)

    find_packages("yp/packages")
    find_packages("proto")
    setup(
        name = name,
        version = version,
        packages = ["yp"] + recursive("yp/packages") + recursive("proto"),
        package_data = {
            name: ["yp/YandexInternalRootCA.crt", "yp/enums.bc"],
        },
        scripts = scripts,

        install_requires = requires,

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "Python library for YP.",
        keywords = "yp python yson",

        long_description = "TODO",

        data_files = data_files,

        include_package_data=True,
    )

