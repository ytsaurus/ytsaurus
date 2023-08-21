#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='yt_setup',
    version='1.0.0',
    packages=find_packages("yt_setup"),
    entry_points={
        "console_scripts": [
            "prepare_python_modules = yt_setup.prepare_python_modules:main",
            "generate_python_proto = yt_setup.generate_python_proto:main",
        ],
    },
)
