import os
import sys
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../scripts/teamcity-build/python"))
from helpers import rmtree, mkdirp

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

