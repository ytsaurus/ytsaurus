PACKAGE_NAME = "ytsaurus-mcp"

MAJOR_VERSION = "0.1"


def main():
    from yt_setup.helpers import recursive, get_package_version

    from setuptools import setup

    version = get_package_version(MAJOR_VERSION)

    setup(
        name=PACKAGE_NAME,
        version=version,
        python_requires=">=3.8",
        entry_points={
            "console_scripts": [
                "mcp_yt_server = yt.mcp.bin.mcp_yt_server:main"
            ],
        },
        packages=["yt.mcp", "yt.mcp.lib", "yt.mcp.bin", "yt.mcp.lib.tools"],

        author="YTsaurus",
        author_email="dev@ytsaurus.tech",
        license="Apache 2.0",
        description="MCP server for YT client.",
        keywords="yt ytsaurus mcp server python client",

        long_description="Implements some YT methods via MCP",

        install_requires=[
            "mcp >= 1.7.1",
            "pydantic >= 2.10.4",
            "ytsaurus-client",
        ],
    )


if __name__ == "__main__":
    main()
