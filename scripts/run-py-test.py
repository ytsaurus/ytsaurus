#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""\
run-py-test.py -- run YT tests using binaries built with `yall' tool.

It accepts all options of py.test (and it calls `exec pytest' after all).
It also recognizes following environment variables:

RUN_PY_TEST_SET_SUID_BIT
    possible values: "1", "0"
  Set suid bits for YT executables.

YT_BUILD_PYTHON_VERSION 
    possible values: "2.7", "3.4" (or any other python version that is installed on machine)
  Use specified version of python to run tests.
"""

import os
import sys
import logging
import shutil
import stat
import subprocess

def set_suid(ya_build):
    expected_mode = (
        stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
        | stat.S_IRGRP | stat.S_IXGRP
        | stat.S_IROTH | stat.S_IXOTH
        | stat.S_ISUID
    )
    for binary in ["ytserver-node", "ytserver-exec", "ytserver-job-proxy", "ytserver-tools"]:
        path = os.path.join(ya_build, binary)
        path_stat = os.stat(path)
        if (
            path_stat.st_uid == 0
            and (path_stat.st_mode & expected_mode) == expected_mode
        ):
            continue
        tmp_path = path + "-tmp"
        # Files in build directory are hardlinks to some stuff inside ~/.ya directory.
        # We don't want change persmissions for this inodes, otherwise ya will go crazy.
        # We detach these files from previous inodes by copying them.
        shutil.copy(path, tmp_path)
        os.rename(tmp_path, path)
        with open("/dev/null", "r") as inf:
            subprocess.check_call(["sudo", "-S", "chown", "root", path], stdin=inf)
            subprocess.check_call(["sudo", "-S", "chmod", "4755", path], stdin=inf)

def parse_bool(s):
    return s.lower() in ["1", "true", "yes"]

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

    if parse_bool(os.environ.get("RUN_PY_TEST_SET_SUID_BIT", "")):
        set_suid(ya_build)

    env = os.environ.copy()
    env["PYTHONPATH"] = "{python}:{yp_python}:{install_dir}:{env_pythonpath}".format(
        python=os.path.join(repo_root, "python"),
        yp_python=os.path.join(repo_root, "yp", "python"),
        install_dir=os.path.join(repo_root, "ya-build"),
        env_pythonpath=os.environ.get("PYTHONPATH", ""))

    env["PATH"] = ya_build + ":" + os.environ["PATH"]
    env["PERL5LIB"] = ya_build + ":" + os.environ.get("PERL5LIB", "")

    args = sys.argv[1:] + [env]
    build_python_version = os.environ.get("YT_BUILD_PYTHON_VERSION", "2.7")
    python = "python{}".format(build_python_version)
    if sys.argv[1:2] in [["--help"], ["-h"]]:
        print __doc__
        print "PyTest Help"
        print "==========="
        sys.stdout.flush()
    os.execlpe(python, python, "-m", "pytest", *args)

if __name__ == "__main__":
    main()
