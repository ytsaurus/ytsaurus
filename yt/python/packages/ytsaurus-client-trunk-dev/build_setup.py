PACKAGE_NAME = "ytsaurus-client"


def main():
    from yt_setup.helpers import recursive

    from setuptools import setup

    setup(
        name="ytsaurus-client",
        version="0.13.999-dev0",
        packages=["yt", "yt.type_info", "yt.wrapper", "yt.yson", "yt.ypath", "yt.skiff", "yt.clickhouse", "yt.cli", "yt.wrapper.schema"] + recursive("yt/packages"),
        package_dir={"yt.packages.requests": "yt/packages/requests"},
        package_data={"yt.packages.requests": ["*.pem"]},
        scripts=[],
        entry_points={
            "console_scripts": [
                "yt = yt.cli.yt_binary:main",
            ],
        },

        author="YTsaurus",
        author_email="dev@ytsaurus.tech",
        description="Python wrapper for YTsaurus system and yson parser.",
        keywords="yt python wrapper mapreduce yson",

        long_description=\
            "It is python library for YTsaurus system that works through http api " \
            "and supports most of the features. It provides a lot of default behaviour in case "\
            "of empty tables and absent paths. Also this package provides mapreduce binary "\
            "(based on python library) that is back compatible with Yamr system.",

        data_files=[],
    )


if __name__ == "__main__":
    main()
