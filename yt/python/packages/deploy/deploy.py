#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import shutil
import sys
from copy import deepcopy

from helpers import get_version, copy_tree, import_file, execute_command
from find_debian_package import find_debian_package
from find_pypi_package import find_pypi_package


def parse_arguments():
    parser = argparse.ArgumentParser(description="Build and deploy debian and/or pypi packages")
    parser.add_argument("--package", required=True, nargs="+")
    parser.add_argument("--package-type", choices=["debian", "pypi", "all"], default="all", help="Packages to build (deploy)")
    parser.add_argument("--deploy", action="store_true", default=True)
    parser.add_argument("--no-deploy", action="store_false", dest="deploy")
    parser.add_argument("--repos", nargs="+", default=None, help="Repositories to upload a debian package")
    parser.add_argument("--keep-going", action="store_true", default=False, help="Continue build other packages when some builds failed")
    parser.add_argument("--python-binary", default=sys.executable, help="Path to python binary")
    return parser.parse_args()


PYPI_REPO = "yandex"
DEBIAN_REPO_TO_PACKAGES = {}
DEBIAN_REPO_TO_PACKAGES["search"] = [
    "yandex-yt-python",
    "yandex-yt-python-tools",
    "yandex-yt-local",
    "yandex-yt-transfer-manager-client",
    "yandex-yp-python",
    "yandex-yp-python-skynet",
    "yandex-yp-local",
]
DEBIAN_REPO_TO_PACKAGES["common"] = DEBIAN_REPO_TO_PACKAGES["search"] + ["yandex-yt-python-proto"]
DEBIAN_REPO_TO_PACKAGES["yt-common"] = DEBIAN_REPO_TO_PACKAGES["common"] + ["yandex-yt-python-fennel", "yandex-yt-fennel"]


def build_debian(package):
    try:
        env = os.environ.copy()
        env.update({
            "DEB": "1",
            "DEB_STRIP_EXCLUDE": ".*"
        })

        execute_command(["dpkg-buildpackage", "-i", "-I", "-rfakeroot"], env=env)
        return True
    except RuntimeError:
        logging.exception("Failed to build debian package")
        return False


def build_pypi(package, python_binary):
    try:
        shutil.copy(os.path.join(package, "setup.py"), ".")
        execute_command([python_binary, "setup.py", "bdist_wheel", "--universal"])
        return True
    except (RuntimeError, IOError):
        logging.exception("Failed to build pypi package")
        return False


def build_package(package, package_type, python_binary, skip_pypi=False):
    logging.info("Building '{}'. Packages to build: {}".format(package, package_type))
    build_ok = True

    if package_type in ("debian", "all"):
        logging.info("Building package {}:debian...".format(package))
        ok = build_debian(package)
        build_ok = ok and build_ok
        logging.info("Build of package {}:debian {}".format(package, "OK" if ok else "FAILED"))

    if package_type in ("pypi", "all") and not skip_pypi:
        logging.info("Building package {}:pypi...".format(package))
        ok = build_pypi(package, python_binary)
        build_ok = build_ok and ok
        logging.info("Build of package {}:pypi {}".format(package, "OK" if ok else "FAILED"))

    return build_ok


def deploy_debian_package(package, version, repos=None):
    if not repos:
        repos = [repo for repo, pkgs in DEBIAN_REPO_TO_PACKAGES.items() if package in pkgs]

    if not repos:
        logging.error(
            "Don't know where to upload debian package '{}'. "
            "Please specify repositories explicitly using --repos".format(package)
        )
        raise SystemExit(1)

    try:
        for repo in repos:
            if find_debian_package(repo, package, version):
                logging.warning("Package {} already uploaded to the repository {}, skipping uploaded".format(package, repo))
                continue

            if repo == "common":
                # used in postprocess.sh in some packages
                os.environ["CREATE_CONDUCTOR_TICKET"] = "true"
            pkg_path = "../{}_{}_amd64.changes".format(package, version)
            logging.info("Uploading to {}...".format(repo))
            execute_command(["dupload", pkg_path, "--force", "--to", repo])
        return True
    except RuntimeError:
        logging.exception("Failed to deploy debian package")
        return False


def _get_pypi_package_name():
    return import_file("setup", "setup.py").PACKAGE_NAME


def deploy_pypi_package(package, python_binary):
    try:
        pypi_package_name = _get_pypi_package_name()
        logging.info("Package {} has name {} in pypi".format(package, pypi_package_name))

        if find_pypi_package(pypi_package_name):
            logging.info("Package {} already uploaded to the repository {}, skipping upload".format(package, PYPI_REPO))
            return True

        execute_command([python_binary, "setup.py", "bdist_wheel", "--universal", "upload", "-r", PYPI_REPO])
        return True
    except RuntimeError:
        logging.exception("Failed to upload pypi package")
        return False


def deploy_package(package, package_type, python_binary, repos=None):
    version = get_version()
    logging.info("Deploying {} package for {} (version {})".format(package_type, package, version))

    if version.endswith("local"):
        logging.info("Package version marked as local, deployment skipped")
        return

    if package_type in ("debian", "all"):
        logging.info("Deploying {}:debian...".format(package))
        ok = deploy_debian_package(package, version, repos=repos)
        logging.info("Deployment of {}:debian {}".format(package, "OK" if ok else "FAILED"))

    if package_type in ("pypi", "all"):
        logging.info("Deploying {}:pypi...".format(package))
        ok = deploy_pypi_package(package, python_binary)
        logging.info("Deployment of {}:pypi {}".format(package, "OK" if ok else "FAILED"))


def clean(python_binary, final=False):
    def remove_path(path):
        if os.path.exists(path):
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.unlink(path)

    os.environ["CREATE_CONDUCTOR_TICKET"] = ""

    for path in [".pybuild", "*.egg-info"]:
        remove_path(path)

    if os.path.exists("setup.py"):
        execute_command([python_binary, "setup.py", "clean"], check=False)

    if os.path.exists("debian/rules"):
        execute_command(["make", "-f", "debian/rules", "clean"], check=False)

    for path in ["stable_versions", "MANIFEST.in", "requirements.txt", "setup.py", "postprocess.py"]:
        remove_path(path)

    if final:
        for path in ["debian", "dist", "__pycache__"]:
            remove_path(path)


def main(args=None):
    if not args:
        args = parse_arguments()

    clean(args.python_binary, final=True)

    exit_code = 0
    for package in args.package:
        copy_tree(package, ".")

        if not build_package(package, args.package_type, args.python_binary, skip_pypi=args.deploy):
            if args.deploy:
                logging.error("One or more builds failed. Package '{}' won't be deployed".format(package))

            exit_code = 1

            if args.keep_going:
                continue

            logging.error("Failed")
            raise SystemExit(exit_code)

        if args.deploy:
            deploy_package(package, args.package_type, args.python_binary, repos=args.repos)

            if os.path.exists("./postprocess.py"):
                postprocess_args = deepcopy(args)
                postprocess_args.create_conductor_ticket = bool(os.environ["CREATE_CONDUCTOR_TICKET"])
                import_file("postprocess", "./postprocess.py").main(postprocess_args)

        clean(args.python_binary, final=args.deploy)

    logging.info("Completed ({})".format("SUCCESSFUL" if not exit_code else "FAILED"))
    raise SystemExit(exit_code)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
