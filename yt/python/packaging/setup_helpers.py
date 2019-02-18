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
