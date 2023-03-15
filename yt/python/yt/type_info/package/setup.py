import os

PACKAGE_NAME = "yandex-type-info"

def main():
    from setuptools import setup
    setup(
        name=PACKAGE_NAME,
        version=os.environ.get("YA_PACKAGE_VERSION"),
        packages=["yandex", "yandex.type_info"],
        author="yt",
        author_email="yt@yandex-team.ru",
        description="Common Yandex types",
        keywords="yandex types",
        include_package_data=True,
    )
if __name__ == "__main__":
    main()
