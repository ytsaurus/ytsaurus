PACKAGE_NAME = "ytsaurus-rpc-driver"

MAJOR_VERSION = "1.0"


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
        packages=["yt_driver_rpc_bindings"],
        package_data={
            "yt_driver_rpc_bindings": [
                "driver_rpc_lib.so",
                "driver_rpc_lib_d.so",
                "driver_rpc_lib.abi3.so",
                "driver_rpc_lib.abi3_d.so",
            ],
        },

        author="YTsaurus",
        author_email="dev@ytsaurus.tech",
        license="Apache 2.0",

        description="C++ RPC driver.",
        long_description=\
            "YTsaurus — is a platform for distributed storage and processing of large amounts of data with support of MapReduce, "\
            "distributed file system and NoSQL key-value storage."\
            "\n\n"\
            "This library provides C++ RPC driver.",
        keywords="yt ytsaurus python bindings driver",

        include_package_data=True,
        distclass=BinaryDistribution,
    )


if __name__ == "__main__":
    main()
