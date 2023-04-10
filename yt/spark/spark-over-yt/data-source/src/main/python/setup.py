from distutils.util import convert_path
import os
import setuptools

ver_path = convert_path('spyt/version.py')
with open(ver_path) as ver_file:
    exec(ver_file.read())

SCRIPTS_PATH = convert_path('deps/bin')
scripts = [os.path.join(SCRIPTS_PATH, x) for x in os.listdir(SCRIPTS_PATH)
           if os.path.isfile(os.path.join(SCRIPTS_PATH, x))]

setuptools.setup(
    name='ytsaurus-spyt',
    version=__version__,
    author='YTsaurus',
    author_email='dev@ytsaurus.tech',
    description='SPYT high-level client',
    url='https://github.com/ytsaurus/ytsaurus/tree/main/yt/spark/spark-over-yt',
    packages=[
        'spyt',
        'spyt.jars'
    ],
    install_requires=[
        'ytsaurus-pyspark=={}'.format(__spark_version__),
        'pyarrow',
        'pyyaml'
    ],
    scripts=scripts,
    license='http://www.apache.org/licenses/LICENSE-2.0',
    package_dir={
        'spyt.jars': 'deps/jars'
    },
    package_data={
        'spyt.jars': ['*.jar']
    },
    include_package_data=True,
)
