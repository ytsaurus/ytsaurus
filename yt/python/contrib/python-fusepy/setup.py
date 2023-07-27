#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup

with open('README') as readme:
    documentation = readme.read()

setup(
    name = 'fusepy',
    version = '2.0.4',

    description = 'Simple ctypes bindings for FUSE',
    long_description = documentation,
    author = 'Giorgos Verigakis',
    author_email = 'verigak@gmail.com',
    maintainer = 'Terence Honles',
    maintainer_email = 'terence@honles.com',
    license = 'ISC',
    py_modules=['fuse'],
    url = 'http://github.com/terencehonles/fusepy',

    classifiers = [
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Operating System :: MacOS',
        'Operating System :: POSIX',
        'Operating System :: Unix',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 3',
        'Topic :: System :: Filesystems',
    ]
)
