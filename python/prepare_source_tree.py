#!/usr/bin/env python
# -*- coding: utf-8 -*-

from prepare_source_tree_helpers import (
    apply_multiple,
    replace,
    replace_symlink,
)

import argparse
import os


PY23_BINARIES = (
    "yp/bin/yp",
    "yp/bin/yp-local",
)


def prepare_yp_python_source_tree(yt_root):
    yp_python_root = os.path.join(yt_root, "yp/python")

    replace(
        os.path.join(yt_root, "contrib/python/prettytable/prettytable.py"),
        os.path.join(yp_python_root, "yp/packages"),
    )

    for binary in PY23_BINARIES:
        binary_path = os.path.join(yp_python_root, binary)
        for suffix in ("2", "3"):
            replace_symlink(binary_path, binary_path + suffix)


def get_default_yt_root():
    return apply_multiple(times=3, func=os.path.dirname, argument=os.path.abspath(__file__))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--yt-root", default=get_default_yt_root())
    args = parser.parse_args()

    prepare_yp_python_source_tree(yt_root=args.yt_root)


if __name__ == "__main__":
    main()
