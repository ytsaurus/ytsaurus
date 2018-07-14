from helpers import get_version

from setuptools import setup

from setuptools.dist import Distribution

class BinaryDistribution(Distribution):
    def is_pure(self):
        return False

def main():
    setup(
        name = "yandex-yt-driver-rpc-bindings",
        version = get_version(),
        packages = ["yt_driver_bindings"],
        package_data = {"yt_driver_bindings": ["driver_lib.so"] },

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "C++ bindings to driver.",
        keywords = "yt python bindings driver",
        include_package_data = True,
        distclass = BinaryDistribution,
    )

if __name__ == "__main__":
    main()
