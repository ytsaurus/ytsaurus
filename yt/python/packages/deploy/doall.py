#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import shutil
import sys
import tempfile

from contextlib import contextmanager
from helpers import copy_tree, change_cwd, execute_command


def parse_arguments():
    parser = argparse.ArgumentParser(description="YT package builder entry point")
    parser.add_argument("--arcadia-root", help="Path to arcadia root")
    parser.add_argument("--yt-path", default="yt/", help="Relative path to YT root")
    parser.add_argument("--package", required=True, nargs="+", help="Name of package to build (for example yandex-yt-python)")
    parser.add_argument("--package-type", choices=["debian", "pypi", "all"], default="all", help="Packages to build (deploy)")
    parser.add_argument("--keep-going", action="store_true", default=False, help="Continue build other packages when some builds failed")
    parser.add_argument("--deploy", action="store_true", default=True, help="Deploy built packages")
    parser.add_argument("--no-deploy", action="store_false", dest="deploy", help="Do not deploy built packages")
    parser.add_argument("--repos", nargs="+", default=None, help="Repositories to upload a debian package")
    parser.add_argument("--work-dir", help="Directory to use. If not specified, temp dir will be created")
    parser.add_argument("--already-prepared", action="store_true", default=False, help="If set, then no source tree preparation will be performed")
    parser.add_argument("--python-binary", default=sys.executable, help="Path to python binary")
    parser.add_argument("--debian-dist-user", default=os.getlogin(), help="Username to access 'dupload.dist.yandex.ru' (dist-dmove role required)")
    parser.add_argument("--verbose", "-v", action="count", default=2)
    return parser.parse_args()


@contextmanager
def work_dir(args):
    if args.already_prepared and not args.work_dir:
        logging.error(
            "Working directory marked as already prepared, but not specified."
            "Use --work-dir to specify path to prepared source tree,"
            " or remove flag --alredy-prepared to automatically prepare it from arcadia sources")
        raise SystemExit(1)

    if not args.work_dir:
        with tempfile.TemporaryDirectory(prefix="yt-package-build-") as temp_dir:
            with change_cwd(temp_dir):
                args.work_dir = temp_dir
                yield
        return

    if not args.already_prepared:
        shutil.rmtree(args.work_dir, ignore_errors=True)
        os.makedirs(args.work_dir)
        # Hack because dpkg-buildpackage puts its artifacts in parent directory
        args.work_dir = os.path.join(args.work_dir, "build")
        os.mkdir(args.work_dir)

    with change_cwd(args.work_dir):
        yield


def prepare_source_tree(args):
    logging.debug("Copying files to working directory")
    yt_dir = os.path.join(args.arcadia_root, args.yt_path)
    yt_python_dir = os.path.join(yt_dir, "python")
    copy_tree(yt_python_dir, ".")

    with change_cwd("prepare_source_tree"):
        execute_command([
            args.python_binary, "prepare_source_tree.py",
            "--arcadia-root", args.arcadia_root,
            "--python-root", args.work_dir,
            "--yt-root", yt_dir
        ])

    copy_tree(os.path.join(yt_python_dir, "packages"), ".")
    copy_tree("./deploy", ".")

    pypi_helpers_path = os.path.join(yt_dir, "teamcity-build/python/python_packaging/pypi_helpers.py")
    if os.path.exists(pypi_helpers_path):
        shutil.copy(pypi_helpers_path, ".")

    logging.info("Source tree is ready")


def main(args=None):
    if not args:
        args = parse_arguments()

    LOG_LEVELS = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=LOG_LEVELS[args.verbose])

    if args.arcadia_root is None and not args.already_prepared:
        logging.error(
            "Path to arcadia root is required to prepare source tree. "
            "Please specify it using --arcadia-root, "
            "or use --already-prepared if the source tree in working directory has already prepared.")
        raise SystemExit(1)

    with work_dir(args):
        logging.info("Working directory is {}".format(os.getcwd()))

        if not args.already_prepared:
            logging.info("Preparing source tree")
            prepare_source_tree(args)
        else:
            logging.info("Running with --already-prepared, skipping source tree preparation")

        logging.debug("Running deploy script")
        from deploy import main as do_deploy
        return do_deploy(args)


if __name__ == "__main__":
    main()
