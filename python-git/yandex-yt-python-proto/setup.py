from helpers import get_version, recursive

from setuptools import setup

def main():
    setup(
        name = "yandex-yt-proto",
        version = get_version(),
        packages = recursive("yt_proto"),

        author = "Andrey Saitgalin",
        author_email = "asaitgalin@yandex-team.ru",
        description = "Python proto files for YT system",
        keywords = "yt python wrapper mapreduce proto",

        long_description = "It is python proto library for YT system"
    )

if __name__ == "__main__":
    main()
