import os
import re
import shutil

# Used to teamcity to import paths.
import teamcity_helpers  # noqa

from teamcity.helpers import rmtree, mkdirp, run, run_captured, cwd

def copy_element(src, dst, elem):
    source = os.path.join(src, elem)
    destination = os.path.join(dst, elem)
    if os.path.isdir(source):
        shutil.copytree(source, destination)
    else:
        shutil.copy(source, destination)

def copy_content(src, dst):
    for elem in os.listdir(src):
        copy_element(src, dst, elem)

def remove_content(path):
    for elem in os.listdir(path):
        elem_path = os.path.join(path, elem)
        if os.path.isfile(elem_path):
            os.remove(elem_path)
        else:
            shutil.rmtree(elem_path)

def clean_path(path):
    if os.path.exists(path):
        rmtree(path)
    mkdirp(path)

def create_if_missing(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def find_file_by_prefix(root, prefix):
    for root, dirs, files in os.walk(root):
        for name in files:
            if name.startswith(prefix):
                return os.path.join(root, name)

def get_ubuntu_codename():
    return re.sub(r"^Codename:\s*", "", run_captured(["lsb_release", "-c"]))
