#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import os
import shutil
import subprocess
import sys
import tempfile

THIS_FILE = os.path.realpath(__file__)
THIS_DIRECTORY = os.path.dirname(THIS_FILE)

REPO_ROOT = os.path.realpath(os.path.join(THIS_DIRECTORY, ".."))
GIT_DEPTH = os.path.join(REPO_ROOT, "git-depth.py")
YA = os.path.realpath(os.path.join(REPO_ROOT, "ya"))

PREFIX_TO_PROJECT = {
    "yandex-yt": "yt",
    "yandex-yp": "yp",
}

class PackageError(RuntimeError):
    pass

class VersionGetter(object):
    def __init__(self, install_dir):
        self.install_dir = install_dir
        self.ya_version_printer = None

    def build_ya_version_printer(self):
        subprocess.check_call([
            YA, "make", "yt/tools/ya_version_printer",
            "--install", self.install_dir
        ], cwd=REPO_ROOT)
        self.ya_version_printer = os.path.join(self.install_dir, "ya_version_printer")

    def get_version(self, project):
        if not self.ya_version_printer:
            self.build_ya_version_printer()

        patch_number = subprocess.check_output([GIT_DEPTH], cwd=REPO_ROOT).strip()
        return subprocess.check_output([
                self.ya_version_printer,
                "--project", project,
                "--patch-number", patch_number,
            ]).rstrip("\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", help="build type (debug, release, etc)", required=True)
    parser.add_argument("--publish-to", default=None)
    parser.add_argument("-Y", "--ya-package-argument", action="append", dest="ya_package_argument_list", default=[])
    parser.add_argument("--override-yt-version")
    parser.add_argument("--override-yp-version")
    parser.add_argument("package_json_description", nargs='+')

    args = parser.parse_args()

    overriden_versions = {
        "yt": args.override_yt_version,
        "yp": args.override_yp_version,
    }

    tmp_install_dir = tempfile.mkdtemp()
    try:
        version_getter = VersionGetter(tmp_install_dir)
        packages_by_version = {}
        for package in args.package_json_description:
            for prefix in PREFIX_TO_PROJECT:
                if os.path.basename(package).startswith(prefix):
                    project = PREFIX_TO_PROJECT[prefix]
                    version = overriden_versions[project]
                    if not version:
                        version = version_getter.get_version(project)
                    packages_by_version.setdefault(version, []).append(package)
                    break
            else:
                raise PackageError("Package {0} has unknown prefix. List of known prefixes: {1}".format(package, PREFIX_TO_PROJECT.keys()))
    finally:
        shutil.rmtree(tmp_install_dir)

    for version, package_list in packages_by_version.iteritems():
        cmd = [
            YA, "package",
            "--build", args.build,
            "--debian",
            "--custom-version", version,
            "--strip",
            "--create-dbg",
        ] + args.ya_package_argument_list + package_list
        if args.publish_to:
            cmd += ["--publish-to", args.publish_to]
        subprocess.check_call(cmd)

if __name__ == "__main__":
    try:
        main()
    except PackageError as e:
        print >>sys.stderr, str(e)
        print >>sys.stderr, "Error occured. Exiting..."
        exit(1)
