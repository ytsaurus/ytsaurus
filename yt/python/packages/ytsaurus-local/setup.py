PACKAGE_NAME = "ytsaurus-local"

MAJOR_VERSION = "0.1"


def main():
    from yt_setup.helpers import prepare_files, get_package_version

    from setuptools import setup
    from setuptools.command.test import test as TestCommand

    version = get_package_version(MAJOR_VERSION)

    class PyTest(TestCommand):
        def finalize_options(self):
            TestCommand.finalize_options(self)
            self.test_args = ["-vs"]
            self.test_suite = True

        def run_tests(self):
            # import here, cause outside the eggs aren't loaded
            import pytest
            pytest.main(self.test_args)

    scripts, data_files = prepare_files([
        "yt/local/bin/yt_local",
        "yt/environment/bin/yt_env_watcher"
    ])

    setup(
        name=PACKAGE_NAME,
        version=version,
        packages=["yt.local", "yt.environment", "yt.environment.api", "yt.environment.migrationlib", "yt.test_helpers"],
        scripts=scripts,

        author="YTsaurus",
        author_email="dev@ytsaurus.tech",
        license="Apache 2.0",
        description="Python libraries and CLI to manage local YTsaurus instances.",
        keywords="yt ytsaurus local mapreduce",

        long_description=\
            "This package contains python library which helps to set up " \
            "fully-functional YTsaurus cluster instance locally. It is designed to be " \
            "flexible and allows to choose desired cluster configuration. " \
            "Also this packages provides yt_local binary (based on python library) " \
            "which can be used to manage local instances manually from command-line.",

        install_requires=[
            "ytsaurus-client>=0.13",
            "attrs==22.2.0",
        ],

        # Using py.test, because it much more verbose
        cmdclass={"test": PyTest},
        tests_require=["pytest"],

        data_files=data_files
    )

if __name__ == "__main__":
    main()
