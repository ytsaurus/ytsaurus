from helpers import get_version, prepare_files

from setuptools import setup

def main():
    requires = ["yandex-yt >= 0.8.4", "sh", "subprocess32"]

    scripts, data_files = prepare_files([
        "yt/tools/bin/add_user/yt_add_user.py",
        "yt/tools/bin/set_account/yt_set_account.py",
        "yt/tools/bin/lock/yt_lock.py",
        "yt/tools/bin/checksum/yt_checksum.py",
        "yt/tools/bin/dump_restore_erase/yt_dump_restore_erase.py"])

    data_files.append(("/etc/yandex_yt_python_tools", ["yt/tools/version"]))

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
