from helpers import get_version, prepare_files

from setuptools import setup

def main():
    scripts, data_files = prepare_files(["yt/fennel/binaries/fennel.py"])

    setup(
        name="yandex-yt-fennel",
        version=get_version(),
        packages = [ "yt.fennel" ],

        scripts = scripts,
        data_files = data_files,

        author="Oleksandr Pryimak",
        author_email="tramsmm@yandex-team.ru",
    )


if __name__ == "__main__":
    main()
