PACKAGE_NAME = "ytsaurus-client"

MAJOR_VERSION = "0.13"


def main():
    from yt_setup.helpers import recursive, get_package_version

    from setuptools import setup

    version = get_package_version(MAJOR_VERSION)

    with open("yt/wrapper/version.py", "w") as version_output:
        version_output.write("VERSION='{0}'".format(version))

    entry_points = {
        "console_scripts": [
            "yt = yt.cli.yt_binary:main",
        ],
    }

    setup(
        name=PACKAGE_NAME,
        version=version,
        packages=["yt", "yt.wrapper", "yt.yson", "yt.ypath", "yt.skiff", "yt.clickhouse", "yt.cli", "yt.type_info", "yt.wrapper.schema"] + recursive("yt/packages"),
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
            "simplejson >=3.18.4, ~=3.18",
            "decorator >=4.4.2, ~=4.4",
            "tqdm >=4.66.1, ~=4.66",
            "argcomplete >=3.2.1, ~=3.1",
            "six >=1.16.0, ~=1.16",
            "charset-normalizer >=3.3.0, ~=3.3",
            "typing-extensions >=4.9.0, ~=4.8",
            "distro >=1.6.0, ~=1.6",
        ],
    )


if __name__ == "__main__":
    main()
