PACKAGE_NAME = "ytsaurus-yson"

MAJOR_VERSION = "0.4"


def main():
    from setuptools import setup
    from setuptools.dist import Distribution

    from yt_setup.helpers import get_package_version

    version = get_package_version(MAJOR_VERSION)

    class BinaryDistribution(Distribution):
        def is_pure(self):
            return False

        # Python 3 specific
        def has_ext_modules(self):
            return True

    setup(
        name=PACKAGE_NAME,
        version=version,
        packages=["yt_yson_bindings"],
        package_data={
            "yt_yson_bindings": [
                "yson_lib.so",
                "yson_lib_d.so",
                "yson_lib.abi3.so",
                "yson_lib.abi3_d.so",
            ],
        },

        author="YTsaurus",
        author_email="dev@ytsaurus.tech",
        license="Apache 2.0",

        description="C++ bindings for YSON.",
        keywords="yt python bindings yson",

        include_package_data=True,
        distclass=BinaryDistribution,

        install_requires=[
            "ytsaurus-client >= 0.12.0",
        ],
    )


if __name__ == "__main__":
    main()
