import os
import sys

from distutils.core import setup

setup(
    name="fennel",
    version="0.0",
    author="Oleksandr Pryimak",
    author_email="tramsmm@yandex-team.ru",
    package_dir = {"": "../yt"},
    py_modules=["fennel"]
)
