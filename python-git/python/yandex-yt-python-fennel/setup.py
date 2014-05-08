import os
import subprocess

from setuptools import setup

def main():
    scripts = []
    data_files = []
    files = [
        "yt/fennel/binaries/fennel.py"
    ]
    for file in files:
        # in egg and debian cases strategy of binary distribution is different
        if "EGG" in os.environ:
            scripts.append(file)
        else:
            data_files.append(("/usr/bin", [file]))

    version = subprocess.check_output("dpkg-parsechangelog | grep Version | awk '{print $2}'", shell=True)

    setup(
        name="fennel",
        version=version,
        packages = [ "yt.fennel" ],

        scripts = files,
        data_files = data_files,

        author="Oleksandr Pryimak",
        author_email="tramsmm@yandex-team.ru",
    )


if __name__ == "__main__":
    main()
