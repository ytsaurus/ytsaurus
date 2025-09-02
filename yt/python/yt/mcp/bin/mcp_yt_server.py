from yt.mcp.lib.tool_runner_mcp import YTToolRunnerMCP

from yt.mcp.lib.tools.list_dir import ListDir, Search
from yt.mcp.lib.tools.get_attributes import GetAttributes
from yt.mcp.lib.tools.check_is_paths_exists import CheckIsPathsExists
from yt.mcp.lib.tools.admin import GetProxy
from yt.mcp.lib.tools.account import CheckPermissions, AccountProperty
from yt.mcp.lib.tools.common_cypress import CommonCypress

import logging
import argparse


def get_app_args():
    parser = argparse.ArgumentParser(
        prog='YT MCP server',
    )
    parser.add_argument("--server-transport", type=str, default="stdio", choices=["stdio", "sse"])
    parser.add_argument("--log-file", type=str, default=None)
    parser.add_argument("--log-level", type=str, default="ERROR", choices=["INFO", "ERROR", "DEBUG"])
    parser.add_argument("--yt-token-file", type=str, default=None, help="Path to yt auth token")

    args = parser.parse_args()

    return args


def main():

    app_args = get_app_args()

    mcp_runner = YTToolRunnerMCP()

    mcp_runner.configure_logging(
        level=logging.getLevelName(app_args.log_level),
        file_name=app_args.log_file,
    )

    mcp_runner.configure_yt(
        token_file=app_args.yt_token_file,
    )

    mcp_runner.attach_tools(
        [
            # list_dir
            ListDir(),
            Search(),
            # get_attributes
            GetAttributes(),
            # check_is_paths_exists
            CheckIsPathsExists(),
            # admin
            GetProxy(),
            # account
            CheckPermissions(),
            AccountProperty(),
            # common_cypress
            CommonCypress(),
        ]
    )

    mcp_runner.start(
        transport=app_args.server_transport,
    )


if __name__ == "__main__":
    main()
