#!/usr/bin/env python3

import os

from setuptools import setup

from yt_setup.helpers import recursive

setup(
    name='ytsaurus-odin',
    version='1.0.0',
    packages=recursive("yt_odin") + recursive("yt_odin_checks"),
    scripts=[
        os.path.join("yt_odin/bin/", script) for script in os.listdir("yt_odin/bin/")
    ] + [
        os.path.join("yt_odin_checks/bin/", script) for script in os.listdir("yt_odin_checks/bin/")
    ],
)
