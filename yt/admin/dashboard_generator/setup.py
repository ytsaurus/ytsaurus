from setuptools import setup, find_packages

setup(
    name="ytsaurus-dashboard-generator",
    version="0.0.1",
    packages=find_packages(),

    author="YTsaurus",
    author_email="dev@ytsaurus.tech",
    license="Apache 2.0",
    description="Python libraries and CLI to manage local YTsaurus instances.",
    keywords="yt ytsaurus dashboards",

    long_description=\
        "This package contains python library which helps to generate dashboards "\
        "for Grafana and Monitoring backends.",

    install_requires=[
        "requests>=2.31.0, ~=2.31",
        "tabulate>=0.9.0, ~=0.9",
        "colorama>=0.4.6, ~=0.4",
        "lark-parser>=0.11.2, ~=0.11",
        "grpcio>=1.54.2, ~=1.54",
        "protobuf>=3.18.1, ~=3.18",
    ],
)
