#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
import tempfile
from contextlib import contextmanager

from .os_helpers import cd


def parse_arguments():
    parser = argparse.ArgumentParser(description="YT package builder entry point")
    parser.add_argument("--package", required=True, nargs="+", help="Name of package to build (for example yandex-yt-python)")
    parser.add_argument("--package-type", choices=["debian", "pypi", "all"], default="all", help="Packages to build (deploy)")
    parser.add_argument("--keep-going", action="store_true", default=False, help="Continue build other packages when some builds failed")
    parser.add_argument("--deploy", action="store_true", default=True, help="Deploy built packages")
    parser.add_argument("--no-deploy", action="store_false", dest="deploy", help="Do not deploy built packages")
    parser.add_argument("--repos", nargs="+", default=None, help="Repositories to upload a debian package")
    parser.add_argument("--work-dir", help="Directory to use. If not specified, temp dir will be created")
    parser.add_argument("--python-binary", default=sys.executable, help="Path to python binary")
    parser.add_argument("--debian-dist-user", default=os.environ.get("USER"), help="Username to access 'dupload.dist.yandex.ru' (dist-dmove role required)")
    parser.add_argument("--verbose", "-v", action="count", default=2)
    return parser.parse_args()


@contextmanager
def work_dir(args):
    if not args.work_dir:
        with tempfile.TemporaryDirectory(prefix="yt-package-build-") as temp_dir:
            with cd(temp_dir):
                args.work_dir = temp_dir
                yield
        return

    with cd(args.work_dir):
        yield


def main(args=None):
    if not args:
        args = parse_arguments()

    LOG_LEVELS = [logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=LOG_LEVELS[args.verbose])

    with work_dir(args):
        logging.info("Working directory is {}".format(os.getcwd()))

        logging.debug("Running deploy script")
        from .deploy import main as do_deploy
        return do_deploy(args)


if __name__ == "__main__":
    main()
