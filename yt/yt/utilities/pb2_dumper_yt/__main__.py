#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
pb2_dumper is an arcadia-python-binary tool that allows to dump _pb2.py scripts that
are compiled into it (useful to build system python packages that require _pb2.py files).
"""

import library.python.resource as resource

import argparse
import os
import re
import shutil


def rmrf(path):
    if os.path.isdir(path):
        shutil.rmtree(path)
    else:
        os.remove(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("output_directory")
    parser.add_argument("--filter", action="append")
    parser.add_argument("--fake-dependency")
    args = parser.parse_args()

    PREFIX = "resfs/file/py/"

    to_dump = {}
    for key, content in resource.iteritems():
        # Example of resource name:
        #   /resfs/file/py/yt/core/misc/proto/error_pb2.py
        if not key.startswith(PREFIX):
            continue
        module_path = key[len(PREFIX):]
        if any(module_path.startswith(p) for p in ["yt_proto/"]) and module_path.endswith(".py"):
            to_dump[module_path] = content

    top_level_entries_created_by_us = set()

    for directory in ("yt_proto",):
        path = os.path.join(args.output_directory, directory)
        if os.path.exists(path):
            rmrf(path)

    for module_path, content in to_dump.items():
        module_matches_filter = not args.filter or any(re.match(f, module_path.replace("/", ".")) for f in args.filter)
        if not module_matches_filter:
            continue
        module_parts = module_path.split("/")
        assert module_path
        if module_parts[0] != "yt_proto":
            raise RuntimeError("Unexpected module: {0}".format(module_path))

        curpath = args.output_directory
        for e in module_parts[:-1]:
            curpath = os.path.join(curpath, e)
            if not os.path.exists(curpath):
                os.mkdir(curpath)
                with open(os.path.join(curpath, "__init__.py"), "w"):
                    pass

        with open(os.path.join(curpath, module_parts[-1]), "wb") as outf:
            outf.write(content)
