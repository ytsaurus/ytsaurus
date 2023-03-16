PACKAGE_NAME = "yandex-yt-tools"

def main():
    from yt_setup.helpers import get_version, prepare_files

    from setuptools import setup

    requires = ["yandex-yt >= 0.8.43", "sh", "subprocess32"]

    scripts, data_files = prepare_files([
        "yt/tools/bin/lock.py",
        "yt/tools/bin/checksum.py",
        "yt/tools/bin/yt_dump_restore_erase.py"])

    setup(
        name=PACKAGE_NAME,
        version=get_version(),
        packages=["yt.tools"],

        scripts=scripts,
        data_files=data_files,

        install_requires=requires,

        author="Ignat Kolesnichenko",
        author_email="ignat@yandex-team.ru",
        description="Experimental scripts to manage YT. Use these scripts at your own risk.",
        keywords="yt python tools import export mapreduce",
    )

if __name__ == "__main__":
    main()
