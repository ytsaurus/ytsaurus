from setuptools import setup

import os
import subprocess

def main():
    requires =["yandex-yt-python", "python-sh"]

    scripts = []
    data_files = []
    files = ["yt/tools/binaries/import_from_mr.py",
             "yt/tools/binaries/export_to_mr.py",
             "yt/tools/binaries/yt_add_user.py",
             "yt/tools/binaries/yt_set_account.py",
             "yt/tools/binaries/yt_convert_to_erasure.py"]
    for file in files:
        # in egg and debian cases strategy of binary distribution is different
        if "EGG" in os.environ:
            scripts.append(file)
        else:
            data_files.append(("/usr/bin", [file]))

    version = subprocess.check_output("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True)

    setup(
        name = "YtTools",
        version = version,
        packages = ["yt.tools"],

        scripts = scripts,
        data_files = data_files,

        install_requires = requires,

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "Experimental scripts to manage YT. Use these scripts at your own risk.",
        keywords = "yt python tools import export mapreduce",
    )

if __name__ == "__main__":
    main()
