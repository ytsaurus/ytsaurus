#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging


def main():
    this_file_path = os.path.realpath(__file__)
    repo_root = os.path.realpath(os.path.join(os.path.dirname(this_file_path), ".."))

    ya_build = os.path.join(repo_root, "ya-build")
    env = {
        "PYTHONPATH": "{python}:{yp_python}:{install_dir}".format(
            python=os.path.join(repo_root, "python"),
            yp_python=os.path.join(repo_root, "yp", "python"),
            install_dir=os.path.join(repo_root, "ya-build")),
        "PATH": ya_build + ":" + os.environ["PATH"],
    }

    args = sys.argv[1:] + [env]
    os.execlpe("py.test", "py.test", *args)

if __name__ == "__main__":
    main()
