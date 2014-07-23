from setuptools import setup

import os
import subprocess

def main():
    requires =["yandex-yt-python", "python-flask"]

    scripts = []
    data_files = []
    files = ["yt/transfer_manager/yt_transfer_manager"]
    for file in files:
        # in egg and debian cases strategy of binary distribution is different
        if "EGG" in os.environ:
            scripts.append(file)
        else:
            data_files.append(("/usr/bin", [file]))

    version = subprocess.check_output("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True)

    setup(
        name = "YandexYtTransferManager",
        version = version,
        packages = [],

        scripts = scripts,
        data_files = data_files,

        install_requires = requires,

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "Daemon that provides scheduler to copy tables between YT and Yamr clusters",
        keywords = "yt python transfer import export mapreduce",
    )

if __name__ == "__main__":
    main()
