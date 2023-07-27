# coding: utf-8

import os
import sys

import exts.os2


def test_fastwalk_unicode():
    os.mkdir("test1")
    os.mkdir("test1/привет")
    sys.stderr.write("{}\n".format(list(exts.os2.fastwalk("test1"))))
