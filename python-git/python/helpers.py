import os
import subprocess

def get_version():
    return subprocess.check_output("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True).strip()

def prepare_files(files):
    scripts = []
    data_files = []
    for file in files:
        # in egg and debian cases strategy of binary distribution is different
        if "EGG" in os.environ:
            scripts.append(file)
        else:
            data_files.append(("/usr/bin", [file]))
    return scripts, files
