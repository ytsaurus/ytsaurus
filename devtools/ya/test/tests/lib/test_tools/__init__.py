# coding: utf-8

import re


def mkfile(filename, data):
    with open(filename, "w") as afile:
        afile.write(data)
    return filename


def to_suitable_file_name(name):
    return re.sub("[\[,\],:,\.,/]", "_", name)
