import setuptools

setuptools.setup(
    name="yandex-spyt",
    version="0.0.8",
    author="Alexandra Belousova",
    author_email="sashbel@yandex-team.ru",
    description="Spark over YT high-level client",
    url="https://github.yandex-team.ru/taxi-dwh/spark-over-yt",
    packages=setuptools.find_packages(),
    install_requires=[
        "yandex-pyspark==2.4.4.post2",
        "yandex-yt>=0.9.29",
        "pyyaml>=5.3.1"
    ],
)
