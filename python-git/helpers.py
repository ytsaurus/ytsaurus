import os
import sys

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
