#!/usr/bin/env python

from setuptools import setup

setup(
    name='yt_setup',
    version='1.0.0',
    packages=["yt_setup"],
    package_dir={"yt_setup": "yt_setup"},
    install_requires=["setuptools"],
    entry_points={
        "console_scripts": [
            "prepare_python_modules = yt_setup.prepare_python_modules:main",
            "generate_python_proto = yt_setup.generate_python_proto:main",
        ],
    },
)
