#!/usr/bin/env python
# -*- coding: utf-8 -*-

try:
    from setuptools import setup
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup

from om_configuration import version, requires_om

setup(name='om_configuration',
      version=version(),
      author='Dmitry Agaphonov',
      author_email='daga@yandex-team.ru',
      description='Om machine-specific configuration files',
      url='https://wiki.yandex-team.ru/PoiskovajaPlatforma/Build/Unix',

      zip_safe=False,
      install_requires = [ 'setuptools', 'om%s' % requires_om() ],

      packages=[ 'om_configuration' ],
      package_data = { 'om_configuration': [ 'data/*' ] },
      exclude_package_data = { '': [ 'CVS', '*/CVS' ] },
      )
