PACKAGE_NAME = "ytsaurus-client"
VERSION = "0.13.1"


def main():
    from yt_setup.helpers import recursive

    from setuptools import setup

    with open("yt/wrapper/version.py", "w") as version_output:
        version_output.write("VERSION='{0}'".format(VERSION))

    entry_points = {
        "console_scripts": [
            "yt = yt.cli.yt_binary:main",
        ],
    }

    setup(
        name=PACKAGE_NAME,
        version=VERSION,
        packages=["yt", "yt.wrapper", "yt.yson", "yt.ypath", "yt.skiff", "yt.clickhouse", "yt.cli", "yt.wrapper.schema"] + recursive("yt/packages"),
        package_dir={"yt.packages.requests": "yt/packages/requests"},
        package_data={"yt.packages.requests": ["*.pem"]},
        entry_points=entry_points,

        author="YTsaurus",
        author_email="dev@ytsaurus.tech",
        license="Apache 2.0",
        description="Python client for YTsaurus system and miscellaneous libraries.",
        keywords="yt ytsaurus python client mapreduce yson ypath",

        long_description=\
            "YTsaurus — is a platform for distributed storage and processing of large amounts of data with support of MapReduce, "\
            "distributed file system and NoSQL key-value storage."\
            "\n\n"\
            "This library provides python client for YTsaurus, that provides python-friendly mechanism "\
            "for running operations, reading/writing data to the cluster and most of the other features.",

        install_requires=[
            "simplejson==3.18.3",
            "decorator==4.4.2",
            "tqdm==4.64.1",
            "argcomplete==2.0.0",
            "six==1.16.0",
            "charset-normalizer==2.0.10",
            "typing-extensions==4.5.0",
        ],
    )


if __name__ == "__main__":
    main()
