PACKAGE_NAME = "yandex-yt-proto"
VERSION = "1.0.0"

def main():
    from yt_setup.helpers import recursive

    from setuptools import setup

    setup(
        name=PACKAGE_NAME,
        version=VERSION,
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
