from setuptools import setup
from distutils.extension import Extension

import os
import subprocess

def main():
    version = subprocess.check_output("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True)

    setup(
        name = "YtYsonBindings",
        version = version,
        packages = ["yt.bindings.yson"],
        package_data = {"yt.bindings.yson": ["yson_lib.so"] },

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "C++ bindings to yson.",
        keywords = "yt python bindings yson",
    )

if __name__ == "__main__":
    main()
