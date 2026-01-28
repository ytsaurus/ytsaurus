PACKAGE_NAME = "ytsaurus-admin"

MAJOR_VERSION = "0.1"


def main():
    from yt_setup.helpers import get_package_version

    from setuptools import setup

    version = get_package_version(MAJOR_VERSION)

    setup(
        name=PACKAGE_NAME,
        version=version,
        python_requires=">=3.8",
        packages=["yt.admin"],
        py_modules=[],

        author="YTsaurus",
        author_email="dev@ytsaurus.tech",
        license="Apache 2.0",
        description="Admin tools for YTsaurus.",
        keywords="yt ytsaurus admin tools",

        long_description="Admin tools for YTsaurus.",

        install_requires=[
            "ytsaurus-client",
            "python-dateutil",
        ],
    )


if __name__ == "__main__":
    main()