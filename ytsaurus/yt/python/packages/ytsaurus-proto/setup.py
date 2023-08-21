PACKAGE_NAME = "yandex-yt-proto"

MAJOR_VERSION = "1.0"


def main():
    from yt_setup.helpers import recursive, get_package_version

    from setuptools import setup

    version = get_package_version(MAJOR_VERSION)

    setup(
        name=PACKAGE_NAME,
        version=version,
        packages=recursive("yt_proto"),

        author="YTsaurus",
        author_email="dev@ytsaurus.tech",
        license="Apache 2.0",
        description="Python proto files for YT system",
        keywords="yt python wrapper mapreduce proto",

        long_description="It is python proto library for YTsaurus system.",
    )


if __name__ == "__main__":
    main()
