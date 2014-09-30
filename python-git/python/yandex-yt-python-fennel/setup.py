from helpers import get_version, prepare_files

from setuptools import setup

def main():
    scripts, data_files = prepare_files(["yt/fennel/binaries/fennel.py"])

    version = get_version()
    with open("yt/fennel/version.py", "w") as version_output:
        version_output.write("VERSION='{0}'\n".format(version))

    setup(
        name="yandex-yt-fennel",
        version=version,
        packages = [ "yt.fennel" ],

        scripts = scripts,
        data_files = data_files,

        author="Oleksandr Pryimak",
        author_email="tramsmm@yandex-team.ru",
    )


if __name__ == "__main__":
    main()
