#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import logging


def main():
    this_file_path = os.path.realpath(__file__)
    repo_root = os.path.realpath(os.path.join(os.path.dirname(this_file_path), ".."))

    ya_build = os.path.join(repo_root, "ya-build")
    if not os.path.exists(ya_build) and "-h" not in sys.argv[1:] and "--help" not in sys.argv[1:]:
        print >>sys.stderr, "ya-build directory cannot be found in repo root, tests are going to fail"
        print >>sys.stderr, "Please run (from repo root directory ):"
        print >>sys.stderr, "  $ {repo_root}/yall --install {ya_build}".format(repo_root=repo_root, ya_build=ya_build)
        print >>sys.stderr, "More convenient way is to configure yall so it will always use this install directory."
        print >>sys.stderr, "  https://wiki.yandex-team.ru/yt/internal/ya/"
        print >>sys.stderr, "ERROR occurred. Exiting..."
        exit(1)
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
