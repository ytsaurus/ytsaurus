"""
Setup for PySpark type annotations.

Notes:

- To avoid manual PYTHONPATH modification the package installation will overlay PySpark installation. If this is not acceptable please download source and add it to PYTHONPATH manually.

"""


from setuptools import setup
import os
import sys

# find_packages doesn't seem to handle stub files
# so we'll enumarate manually
src_path = os.path.join('third_party', '3')

def list_packages(src_path=src_path):
    for root, _, _ in os.walk(os.path.join(src_path, 'pyspark')):
        yield '.'.join(os.path.relpath(root, src_path).split(os.path.sep))


setup(
    name='pyspark-stubs',
    package_dir={'': src_path},
    version='2.3.0.post2',
    description='A collection of the Apache Spark stub files',
    long_description=(open('README.rst').read() if os.path.exists('README.rst')
                        else ''),
    packages=list(list_packages()),
    package_data={
        '': ['*.pyi']
    },
    install_requires=['pyspark>=2.3.0<2.4.0']
)
