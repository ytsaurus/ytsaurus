#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup
    
from om.version import version

setup(name='om',
      version=version(),
      author='Dmitry Agaphonov',
      author_email='daga@yandex-team.ru',
      description='CMake+Make convenient wrapper',
      long_description="""
      Om is a convenient wrapper for CMake build process.  It aggregates calls
      to CMake and Make to a single command.  It runs from a usual source
      directory but not from a temporary build directory.
      """,
      url='https://wiki.yandex-team.ru/PoiskovajaPlatforma/Build/Unix',

      zip_safe=False,
      install_requires = [ 'setuptools' ],

      packages=[ 'om' ],
      package_data = { 'om': [ 'data/*' ] },
      exclude_package_data = { '': [ 'CVS', '*/CVS' ] },

      entry_points = { 'console_scripts': [ 'om = om.main:main' ] },
      )
