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
    name='yandex-spyt',
    version=__version__,
    author='Alexandra Belousova',
    author_email='sashbel@yandex-team.ru',
    description='Spark over YT high-level client',
    url='https://github.yandex-team.ru/taxi-dwh/spark-over-yt',
    packages=[
        'spyt',
        'spyt.jars'
    ],
    install_requires=[
        'yandex-pyspark=={}'.format(__spark_version__),
        'yandex-yt>=0.9.29',
        'pyarrow==0.15.1',
        'pyyaml'
    ],
    scripts=scripts,
    package_dir={
        'spyt.jars': 'deps/jars'
    },
    package_data={
        'spyt.jars': ['*.jar']
    },
    include_package_data=True,
)
