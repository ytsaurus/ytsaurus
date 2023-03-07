import os
import sysconfig

PACKAGE_NAME = "yandex-yt-yson-bindings"

def main():
    from setuptools import setup
    from setuptools.dist import Distribution
    from setup_helpers import get_version

    if "PYTHON_SO_ABI" in os.environ:
        get_config_vars_original = sysconfig.get_config_vars
        def get_config_vars_patch():
            result = get_config_vars_original()
            result["SOABI"] = os.environ["PYTHON_SO_ABI"]
            return result
        sysconfig.get_config_vars = get_config_vars_patch

    class BinaryDistribution(Distribution):
        def is_pure(self):
            return False

        # Python 3 specific
        def has_ext_modules(self):
            return True

    setup(
        name = PACKAGE_NAME + os.environ.get("PYTHON_SUFFIX", ""),
        version = get_version(),
        packages = ["yt_yson_bindings"],
        package_data = {"yt_yson_bindings": ["yson_lib.so", "yson_lib_d.so",
                                             "yson_lib.abi3.so", "yson_lib.abi3_d.so"]},
        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "C++ bindings to yson.",
        keywords = "yt python bindings yson",
        include_package_data = True,
        distclass = BinaryDistribution,
        install_requires = ["yandex-yt >= 0.7.35-0"],
    )

if __name__ == "__main__":
    main()
