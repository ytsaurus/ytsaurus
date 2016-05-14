from helpers import get_version

from setuptools import setup

from setuptools.dist import Distribution

import os
import shutil

class BinaryDistribution(Distribution):
    def is_pure(self):
        return False

def main():
    setup(
        name = "yandex-yt-yson-bindings",
        version = get_version(),
        packages = ["yt_yson_bindings"],
        package_data = {"yt_yson_bindings": ["yson_lib.so"] },

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "C++ bindings to yson.",
        keywords = "yt python bindings yson",
        include_package_data = True,
        distclass = BinaryDistribution,
    )

if __name__ == "__main__":
    main()
