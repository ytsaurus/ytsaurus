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
        # In egg and debian cases strategy of binary distribution is different
        if "DEB" in os.environ:
            data_files.append(("/usr/bin", [file + file_name_suffix]))
        else:
            scripts.append(file + file_name_suffix)
    return scripts, data_files
