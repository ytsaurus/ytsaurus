#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import shutil
import sys
from copy import deepcopy

from .os_helpers import cp_r, cp
from .helpers import get_version, get_version_branch, import_file, execute_command
from .find_debian_package import find_debian_package, fetch_package_versions
from .find_pypi_package import find_pypi_package


def parse_arguments():
    parser = argparse.ArgumentParser(description="Build and deploy debian and/or pypi packages")
    parser.add_argument("--package", required=True, nargs="+")
    parser.add_argument("--package-type", choices=["debian", "pypi", "all"], default="all", help="Packages to build (deploy)")
    parser.add_argument("--deploy", action="store_true", default=True)
    parser.add_argument("--no-deploy", action="store_false", dest="deploy")
    parser.add_argument("--repos", nargs="+", default=None, help="Repositories to upload a debian package")
    parser.add_argument("--keep-going", action="store_true", default=False, help="Continue build other packages when some builds failed")
    parser.add_argument("--python-binary", default=sys.executable, help="Path to python binary")
    parser.add_argument("--debian-dist-user", default=os.environ.get("USER"), help="Username to access 'dupload.dist.yandex.ru' (dist-dmove role required)")
    return parser.parse_args()


PYPI_REPO = "yandex"
DEBIAN_REPO_TO_PACKAGES = {}
DEBIAN_REPO_TO_PACKAGES["common"] = [
    "yandex-yt-python",
    "yandex-yt-python-tools",
    "yandex-yt-local",
    "yandex-yt-transfer-manager-client",
    "yandex-yp-python",
    "yandex-yp-python-skynet",
    "yandex-yp-local",
    "yandex-yt-python-proto",
]
DEBIAN_REPO_TO_PACKAGES["yt-common"] = DEBIAN_REPO_TO_PACKAGES["common"] + [
    "yandex-yt-python-fennel",
    "yandex-yt-fennel",
]


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
        cp(os.path.join(package, "setup.py"), ".")
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


def deploy_debian_package(package, version, version_branch, debian_dist_user=None, repos=None):
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
                logging.warning("Package {} already uploaded to the repository {}, skipping upload".format(package, repo))
                continue

            pkg_path = "../{}_{}_amd64.changes".format(package, version)
            logging.info("Uploading to {}...".format(repo))
            execute_command(["dupload", pkg_path, "--force", "--to", repo])

        if version_branch != "unstable":
            if not debian_dist_user:
                raise RuntimeError("debian_dist_user is required to move package between branches, but it wasn't specified")

            for repo in repos:
                package_versions = [entry for entry in fetch_package_versions(repo, package) if entry[0] == version]
                if len(package_versions) != 1:
                    raise RuntimeError(
                        "Expected to find package {package} (v{version}) in exactly one branch inside repo {repo}, "
                        "but found in {entries}".format(
                            package=package,
                            version=version,
                            repo=repo,
                            entries=package_versions,
                        )
                    )

                current_branch = package_versions[0][1]

                logging.info(
                    "Package {package} found in repository {repo} on branch {current_branch}, "
                    "target branch is {target_branch}".format(
                        package=package,
                        repo=repo,
                        current_branch=current_branch,
                        target_branch=version_branch,
                    )
                )

                if current_branch == version_branch:
                    continue

                dmove_command = "sudo dmove {repo} {version_branch} {package} {version} {current_branch}".format(
                    repo=repo,
                    version_branch=version_branch,
                    package=package,
                    version=version,
                    current_branch=current_branch,
                )

                remote_cmd = ["ssh", "-l", debian_dist_user, "dupload.dist.yandex.ru", dmove_command]
                _, stdout, _ = execute_command(remote_cmd, capture_output=True)
                if "Dmove successfully complete" not in stdout:
                    logging.error("Dmove failed with stdout: {}".format(stdout))
                    raise RuntimeError("Dmove failed")

                logging.info("Successfully moved {package} to {repo}~{branch}".format(package=package, repo=repo, branch=version_branch))
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


def deploy_package(package, package_type, python_binary, debian_dist_user=None, repos=None):
    version = get_version()
    version_branch = get_version_branch(version)
    logging.info("Deploying {} package for {} (version {} branch {})".format(package_type, package, version, version_branch))

    if version.endswith("local"):
        logging.info("Package version marked as local, deployment skipped")
        return

    if package_type in ("debian", "all"):
        logging.info("Deploying {}:debian...".format(package))
        ok = deploy_debian_package(package, version, version_branch, debian_dist_user=debian_dist_user, repos=repos)
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

        package_dir = package
        assert os.path.exists(package_dir)
        for child in os.listdir(package_dir):
            cp_r(os.path.join(package_dir, child), ".")

        if not build_package(package, args.package_type, args.python_binary, skip_pypi=args.deploy):
            if args.deploy:
                logging.error("One or more builds failed. Package '{}' won't be deployed".format(package))

            exit_code = 1

            if args.keep_going:
                continue

            logging.error("Failed")
            raise SystemExit(exit_code)

        if args.deploy:
            deploy_package(
                package,
                args.package_type,
                args.python_binary,
                debian_dist_user=args.debian_dist_user,
                repos=args.repos,
            )

            if os.path.exists("./postprocess.py"):
                postprocess_args = deepcopy(args)
                import_file("postprocess", "./postprocess.py").main(postprocess_args)

        clean(args.python_binary, final=args.deploy)

    logging.info("Completed ({})".format("SUCCESSFUL" if not exit_code else "FAILED"))
    raise SystemExit(exit_code)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
