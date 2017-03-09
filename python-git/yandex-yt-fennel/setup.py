from helpers import get_version

from setuptools import setup

def main():
    setup(
        name = "yandex-yt-fennel",
        version = get_version(),
        packages = ["yt.fennel"],

        scripts = ["yt/fennel/bin/new_fennel.py", "yt/fennel/bin/fennel_rotate.py"],

        author = "Kolesnichenko Ignat",
        author_email = "ignat@yandex-team.ru",
    )

if __name__ == "__main__":
    main()
