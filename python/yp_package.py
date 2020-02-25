def setup_package(name, python_dependent_requires):
    from helpers import get_version, prepare_files

    from setuptools import setup, find_packages

    try:
        from itertools import imap
    except ImportError:  # Python 3
        imap = map

    def recursive(path):
        prefix = path.strip("/").replace("/", ".")
        return list(imap(lambda package: prefix + "." + package, find_packages(path))) + [prefix]

    requires = [
        "yandex-yt >= 0.9.26",
        "yandex-yt-proto",
        "protobuf >= 3.2.1",
        "grpcio == 1.16.0rc1",
    ] + python_dependent_requires

    version = get_version()

    binaries = ["yp/bin/yp", "yp/bin/yp-local"]

    scripts, data_files = prepare_files(binaries)

    find_packages("yp/packages")
    find_packages("yp/data_model")
    find_packages("yp_proto")
    setup(
        name=name,
        version=version,
        packages=["yp"]
        + recursive("yp/packages")
        + recursive("yp/data_model")
        + recursive("yp_proto"),
        package_data={"yp": ["YandexInternalRootCA.crt", "enums.bc"],},
        scripts=scripts,
        install_requires=requires,
        author="Ignat Kolesnichenko",
        author_email="ignat@yandex-team.ru",
        description="Python library for YP.",
        keywords="yp python yson",
        long_description="Python library that implements client for YP system",
        data_files=data_files,
        include_package_data=True,
    )
