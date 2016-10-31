import os
import sys

try:
    import subprocess32 as subprocess
except ImportError:
    import subprocess

def get_version():
    if os.path.exists("VERSION"):
        return open("VERSION").read().strip()
    version = subprocess.check_output("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True)
    if sys.version_info[0] >= 3:
        version = version.decode("ascii")
    return version.strip()

def prepare_files(files):
    scripts = []
    data_files = []
    for file in files:
        file_name_suffix = str(sys.version_info[0])
        # In egg and debian cases strategy of binary distribution is different
        if "DEB" in os.environ:
            data_files.append(("/usr/bin", [file + file_name_suffix]))
        else:
            scripts.append(file + file_name_suffix)
    return scripts, data_files
