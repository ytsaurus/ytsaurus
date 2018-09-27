#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import subprocess
import json
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src_root")

    args = parser.parse_args()
    output = subprocess.check_output(["git", "ls-files", "--recurse-submodules"], cwd=args.src_root)
    file_list = output.strip("\n").split('\n')
    commit = subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=args.src_root)
    commit = commit.strip("\n")

    package = {
        "meta": {
            "name": "yandex-yt-src",
            "maintainer": "YT Team <yt@yandex-team.ru>",
            "description": "YT. This package provides yt sources",
            "version": "{revision}",
            "pre-depends": [],
            "depends": ["libc6 (>= 2.15)"],
            "conflicts": ["yandex-yt"],
            "homepage": "https://wiki.yandex-team.ru/yt/",
        },
        "data": []
    }
    for path in file_list:
        package["data"].append(
            {
                "source": {
                    "type": "ARCADIA",
                    "path": path,
                },
                "destination": {
                    "path": "/share/yandex-yt/source/{commit}/{path}".format(commit=commit, path=path)
                }
            }
        )
    json.dump(package, sys.stdout, indent=4)

if __name__ == "__main__":
    main()
