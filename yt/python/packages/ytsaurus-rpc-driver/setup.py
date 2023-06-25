PACKAGE_NAME = "ytsaurus-rpc-driver"
VERSION = "1.0.0"

def main():
    from setuptools import setup
    from setuptools.dist import Distribution

    class BinaryDistribution(Distribution):
        def is_pure(self):
            return False

        # Python 3 specific
        def has_ext_modules(self):
            return True

    setup(
        name=PACKAGE_NAME,
        version=VERSION,
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
        keywords="yt ytsaurus python bindings driver",

        include_package_data=True,
        distclass=BinaryDistribution,
    )


if __name__ == "__main__":
    main()
