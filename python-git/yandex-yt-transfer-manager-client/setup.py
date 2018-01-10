from helpers import get_version, prepare_files

import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = ["-vs"]
        self.test_suite = True

    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import pytest
        pytest.main(self.test_args)

def main():
    requires = ["yandex-yt >= 0.8.25"]

    if sys.version_info[:2] <= (2, 6):
        requires.append("argparse")

    scripts, data_files = prepare_files([
        "yt/transfer_manager/client/bin/transfer-manager",
        "yt/transfer_manager/client/bin/transfer-manager-check"
    ])

    version = get_version()
    with open("yt/transfer_manager/client/version.py", "w") as v_out:
        v_out.write("VERSION='{0}'".format(version))

    setup(
        name = "yandex-yt-transfer-manager-client",
        version = get_version(),
        packages = ["yt.transfer_manager", "yt.transfer_manager.client"],

        scripts = scripts,
        data_files = data_files,

        install_requires = requires,

        author = "Andrey Saitgalin",
        author_email = "asaitgalin@yandex-team.ru",
        description = \
                "Package contains Python library which wraps HTTP API of Transfer Manager and provides "
                "straightforward interface for managing TM tasks. Also this package includes "
                "transfer-manager binary which provides the same functionality "
                "as library but it can be used from command-line.",
        keywords = "yt python transfer client copy",

        # Using py.test, because it much more verbose
        cmdclass = {"test": PyTest},
        tests_require = ["pytest"]
    )

if __name__ == "__main__":
    main()
