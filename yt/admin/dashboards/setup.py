from setuptools import setup, find_packages

setup(
    name="ytsaurus-dashboards",
    version="0.0.1",
    packages=find_packages(),

    author="YTsaurus",
    author_email="dev@ytsaurus.tech",
    license="Apache 2.0",
    description="Python libraries and CLI to manage local YTsaurus instances.",
    keywords="yt ytsaurus dashboards",

    long_description="This package contains modules to generate various yt dashboards",

    install_requires=[
        "ytsaurus-dashboard-generator",
    ],
)
