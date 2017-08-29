from helpers import get_version

from setuptools import setup

def main():
    setup(
        name = "yandex-yt-transfer-manager",
        version = get_version(),
        packages = ["yt.transfer_manager", "yt.transfer_manager.server"],

        scripts = ["yt/transfer_manager/server/bin/transfer-manager-server/transfer-manager-server"],

        author = "Ignat Kolesnichenko",
        author_email = "ignat@yandex-team.ru",
        description = "Daemon that provides scheduler to copy tables between YT and Yamr clusters",
        keywords = "yt python transfer import export mapreduce",
    )

if __name__ == "__main__":
    main()
