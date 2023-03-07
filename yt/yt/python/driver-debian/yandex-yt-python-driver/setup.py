import os

PACKAGE_NAME = "yandex-yt-driver-bindings"

def main():
    from setuptools import setup
    from setuptools.dist import Distribution
    from setup_helpers import get_version

    class BinaryDistribution(Distribution):
        def is_pure(self):
            return False

        # Python 3 specific
        def has_ext_modules(self):
            return True

    setup(
        name = PACKAGE_NAME + os.environ.get("PYTHON_SUFFIX", ""),
        version = get_version(),
        packages = ["yt_driver_bindings"],
        package_data = {"yt_driver_bindings": ["driver_lib.so",
                                               "driver_lib_d.so",
                                               "driver_lib.abi3.so",
                                               "driver_lib.abi3_d.so"]},
        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "C++ bindings to driver.",
        keywords = "yt python bindings driver",
        include_package_data = True,
        distclass = BinaryDistribution,
    )

if __name__ == "__main__":
    main()
