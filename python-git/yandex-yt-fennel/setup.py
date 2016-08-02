from helpers import get_version, prepare_files

from setuptools import setup

def main():
    scripts, data_files = prepare_files(["yt/fennel/bin/new_fennel.py", "yt/fennel/bin/fennel_rotate.py"])

    setup(
        name="yandex-yt-fennel",
        version=get_version(),
        packages = [ "yt.fennel" ],

        scripts = scripts,
        data_files = data_files,

        author="Kolesnichenko Ignat",
        author_email="ignat@yandex-team.ru",
    )


if __name__ == "__main__":
    main()
