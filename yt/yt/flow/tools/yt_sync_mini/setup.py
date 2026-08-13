PACKAGE_NAME = "ytsaurus-flow-yt-sync-mini"

MAJOR_VERSION = "0.0"


def get_version():
    # In the ytsaurus package-build flow (build_ytsaurus_packages.sh does
    # `pip install -e yt/python/packages` first) versioning goes through
    # yt_setup.helpers, honoring YTSAURUS_PACKAGE_VERSION / YTSAURUS_COMMIT_HASH.
    # For a standalone `pip install <this dir>` yt_setup may be absent, so fall
    # back to the plain major-dev version.
    try:
        from yt_setup.helpers import get_package_version
        return get_package_version(MAJOR_VERSION)
    except ImportError:
        return MAJOR_VERSION + ".0"


def main():
    from setuptools import setup

    version = get_version()

    # Both leaf packages are installed under their real Arcadia import path
    # `yt.yt.flow.library.python.{pipeline_tables,yt_sync_mini}`, so imports are
    # identical to the in-Arcadia (ya.make) build. The intermediate
    # `yt.yt`, `yt.yt.flow`, `yt.yt.flow.library`, `yt.yt.flow.library.python`
    # levels are PEP 420 implicit namespace packages, so this wheel layers onto
    # the `yt` package shipped by ytsaurus-client without file collisions.
    #
    # This setup.py lives at yt/yt/flow/tools/yt_sync_mini/, while the sources
    # live at yt/yt/flow/library/python/{pipeline_tables,yt_sync_mini}; the
    # package_dir entries below point setup.py at them via relative paths.
    setup(
        name=PACKAGE_NAME,
        version=version,
        python_requires=">=3.8",
        packages=[
            "yt.yt.flow.library.python.pipeline_tables",
            "yt.yt.flow.library.python.yt_sync_mini",
        ],
        package_dir={
            "yt.yt.flow.library.python.pipeline_tables": "../../library/python/pipeline_tables",
            "yt.yt.flow.library.python.yt_sync_mini": "../../library/python/yt_sync_mini",
        },

        license="Apache 2.0",

        description="Flow yt_sync_mini + pipeline_tables: bootstrap Cypress objects for Flow pipelines.",
        long_description=\
            "YTsaurus — is a platform for distributed storage and processing of large amounts of data with support of MapReduce, "\
            "distributed file system and NoSQL key-value storage."\
            "\n\n"\
            "This library provides a mini yt_sync replacement (plus the pipeline inner-table schemas and "\
            "presets it needs) that creates the Cypress objects a Flow pipeline requires, without internal tooling.",
        keywords="yt ytsaurus flow yt_sync pipeline",

        install_requires=[
            "ytsaurus-client",
        ],
    )


if __name__ == "__main__":
    main()
