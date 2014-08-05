import os
import subprocess

def get_version():
    if os.path.exists("VERSION"):
        return open("VERSION").read().strip()
    return subprocess.check_output("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True).strip()

def prepare_files(files):
    scripts = []
    data_files = []
    for file in files:
        # in egg and debian cases strategy of binary distribution is different
        if "DEB" in os.environ:
            data_files.append(("/usr/bin", [file]))
        else:
            scripts.append(file)
    return scripts, data_files
