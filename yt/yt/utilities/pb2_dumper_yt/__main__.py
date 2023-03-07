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

    PREFIX = "/py_modules/"

    to_dump = {}
    for key, content in resource.iteritems():
        # Example of resource name:
        #   /py_modules/yt.core.misc.proto.error_pb2
        if not key.startswith(PREFIX):
            continue
        module_name = key[len(PREFIX):]
        if any(module_name.startswith(p) for p in ["yt_proto."]):
            to_dump[module_name] = content

    top_level_entries_created_by_us = set()

    for directory in ("yt_proto",):
        path = os.path.join(args.output_directory, directory)
        if os.path.exists(path):
            rmrf(path)

    for module_name, content in to_dump.iteritems():
        module_matches_filter = not args.filter or any(re.match(f, module_name) for f in args.filter)
        if not module_matches_filter:
            continue
        module_path = module_name.split(".")
        assert module_path
        if module_path[0] == "yt_proto":
            pass
        else:
            raise RuntimeError("Unexpected module: {0}".format(module_name))
        module_path[-1] += ".py"

        curpath = args.output_directory
        for e in module_path[:-1]:
            curpath = os.path.join(curpath, e)
            if not os.path.exists(curpath):
                os.mkdir(curpath)
                with open(os.path.join(curpath, "__init__.py"), "w"):
                    pass

        with open(os.path.join(curpath, module_path[-1]), "w") as outf:
            outf.write(content)
