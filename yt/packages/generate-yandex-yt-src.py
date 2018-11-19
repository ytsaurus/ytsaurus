#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
import os
import re
import subprocess
import sys

def warn_and_exit(msg):
    msg = "Cannot build yandex-yt-src description.\n" + msg.strip("\n")

    sys.stderr.write(msg)
    sys.stderr.write("\n")

    for line in msg.split("\n"):
        print >>sys.stdout, "// " + line
    exit(0)


def parse_git_version(git_version_str):
    m = re.search(r"\b(\d+)[.](\d+)[.](\d+)\b", git_version_str)
    if m is None:
        raise RuntimeError, "Cannot parse git version string: `{}'".format(git_version_str)
    return tuple(int(m.group(i)) for i in xrange(1, 4))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("src_root")
    args = parser.parse_args()

    if not os.path.exists(os.path.join(args.src_root, ".git")):
        warn_and_exit("{} is not a git repository".format(os.path.abspath(args.src_root)))

    try:
        git_version_str = subprocess.check_output(["git", "--version"])
    except subprocess.CalledProcessError as e:
        warn_and_exit("git is not available, cannot create source package")

    if parse_git_version(git_version_str) < (2, 11, 0):
        warn_and_exit(
            "Looks like your version of git doesn't support\n"
            "`--recurse-submodules' option for `ls-files' command\n"
        )

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
