from helpers import get_version

from setuptools import setup

def main():
    setup(
        name = "yandex-yt-yson-bindings",
        version = get_version(),
        packages = ["yt.bindings.yson"],
        package_data = {"yt.bindings.yson": ["yson_lib.so"] },

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "C++ bindings to yson.",
        keywords = "yt python bindings yson",
    )

if __name__ == "__main__":
    main()
