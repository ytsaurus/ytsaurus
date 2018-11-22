from helpers import get_version

from setuptools import setup

from setuptools.dist import Distribution

import os

class BinaryDistribution(Distribution):
    def is_pure(self):
        return False

    # Python 3 specific
    def has_ext_modules(self):
        return True

def main():
    setup(
        name = "yandex-yt-yson-bindings" + os.environ.get("PYTHON_SUFFIX", ""),
        version = get_version(),
        packages = ["yt_yson_bindings"],
        package_data = {"yt_yson_bindings": ["yson_lib.so", "yson_lib.dbg.so",
                                             "yson_lib.abi3.so", "yson_lib.dbg.abi3.so"]},
        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "C++ bindings to yson.",
        keywords = "yt python bindings yson",
        include_package_data = True,
        distclass = BinaryDistribution,
        install_requires = ["yandex-yt >= 0.7.35-0"],
    )

if __name__ == "__main__":
    main()
