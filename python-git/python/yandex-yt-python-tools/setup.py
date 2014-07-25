from helpers import get_version, prepare_files

from setuptools import setup

def main():
    requires =["yandex-yt-python", "python-sh"]

    scripts, data_files = prepare_files([
        "yt/tools/binaries/import_from_mr.py",
        "yt/tools/binaries/export_to_mr.py",
        "yt/tools/binaries/export_to_yt.py",
        "yt/tools/binaries/yt_add_user.py",
        "yt/tools/binaries/yt_set_account.py",
        "yt/tools/binaries/yt_convert_to_erasure.py",
        "yt/tools/binaries/yt_lock.py",
        "yt/tools/binaries/yt_doctor.py"])

    setup(
        name = "yandex-yt-tools",
        version = get_version(),
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
