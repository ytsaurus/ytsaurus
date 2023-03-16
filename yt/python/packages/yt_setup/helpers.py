import os
import sys
import shutil
import logging
import importlib

from contextlib import contextmanager
from setuptools import find_packages

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

try:
    from itertools import imap
except ImportError:  # Python 3
    imap = map


is_debian = "DEB" in os.environ
is_python_egg = "EGG" in os.environ
assert not (is_debian and is_python_egg)


def recursive(path):
    prefix = path.strip("/").replace("/", ".")
    return list(imap(lambda package: prefix + "." + package, find_packages(path))) + [prefix]


def get_version():
    if os.path.exists("VERSION"):
        return open("VERSION").read().strip()
    proc = subprocess.Popen("dpkg-parsechangelog | grep Version | awk '{print $2}'", stdout=subprocess.PIPE, shell=True)
    version, _ = proc.communicate()
    if sys.version_info[0] >= 3:
        version = version.decode("ascii")
    return version.strip()


def get_version_branch(version):
    version = version.split("-")[0]

    branch = "stable"
    if os.path.exists("stable_versions"):
        with open("stable_versions") as fin:
            stable_versions = fin.readlines()

        if version in stable_versions:
            branch = "stable"
        elif version + "~testing" in stable_versions:
            branch = "testing"
        else:
            branch = "unstable"

    return branch


def prepare_files(files, add_major_version_suffix=False):
    scripts = []
    data_files = []
    for file in files:
        file_name_suffix = ""
        if add_major_version_suffix and not file.endswith(".py"):
            file_name_suffix = str(sys.version_info[0])
        # In egg/wheel and debian cases strategy of binary distribution is different.
        if is_debian:
            data_files.append(("/usr/bin", [file + file_name_suffix]))
        else:
            scripts.append(file)
    return scripts, data_files


def copy_tree(src, dst):
    for path in os.listdir(src):
        full_path = os.path.join(src, path)
        if os.path.isfile(full_path):
            logging.debug("Copy file: '{}' -> '{}'".format(full_path, dst))
            shutil.copy(full_path, dst)
        else:
            logging.debug("Copy tree: '{}' -> '{}'".format(full_path, os.path.join(dst, path)))
            shutil.copytree(full_path, os.path.join(dst, path))


@contextmanager
def change_cwd(dir):
    current_dir = os.getcwd()
    os.chdir(dir)
    try:
        yield
    finally:
        os.chdir(current_dir)


def import_file(module_name, path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None:
        raise ModuleNotFoundError(module_name)

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def execute_command(cmd, env=None, check=True, capture_output=False):
    logging.debug("Executing command (check={check}, env={env}):\n $ {cmd}".format(
        cmd=" ".join(["'{}'".format(arg) for arg in cmd]),
        check=check,
        env=env,
    ))

    # if True, the child process' output will directly printed in stdout/stderr
    share_output_fds = logging.root.level <= logging.DEBUG and not capture_output

    proc = subprocess.Popen(
        cmd,
        env=env,
        stdout=subprocess.PIPE if not share_output_fds else None,
        stderr=subprocess.PIPE if not share_output_fds else None,
        universal_newlines=True
    )
    stdout, stderr = proc.communicate()
    returncode = proc.wait()

    if returncode == 0 or not check:
        if not share_output_fds:
            logging.debug("Command stdout: {}".format(stdout))
            logging.debug("Command stderr: {}".format(stderr))
    else:
        if not share_output_fds:
            logging.error("Command stdout: {}".format(stdout))
            logging.error("Command stderr: {}".format(stderr))
        raise RuntimeError("Executing of command '{}' failed. Process exited with code {}".format(cmd, returncode))
    return returncode, stdout, stderr
