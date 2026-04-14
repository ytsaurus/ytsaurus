from yt.mcp.lib.tool_runner_mcp import YTToolRunnerMCP

from yt.mcp.lib.tools.list_dir import ListDir, Search
from yt.mcp.lib.tools.get_attributes import GetAttributes
from yt.mcp.lib.tools.check_is_paths_exists import CheckIsPathsExists
from yt.mcp.lib.tools.admin import GetProxy
from yt.mcp.lib.tools.account import CheckPermissions, AccountProperty
from yt.mcp.lib.tools.common_client import CommonCypress

import argparse
import itertools
import logging


def get_app_args():
    parser = argparse.ArgumentParser(
        prog='YT MCP server',
    )
    parser.add_argument("--server-transport", type=str, default="stdio", choices=["stdio", "sse"])
    parser.add_argument("--log-file", type=str, default=None)
    parser.add_argument("--log-level", type=str, default="ERROR", choices=["INFO", "ERROR", "DEBUG"])
    parser.add_argument("--yt-token-file", type=str, default=None, help="Path to yt auth token")

    parser.add_argument("--show-tools", action="store_true", default=False, help="Show tools and exit")
    parser.add_argument("--tools-common", action="store_const", const="common", default=None, help="Enable common tools")
    parser.add_argument("--tools-account", action="store_const", const="account", default=None, help="Enable account tools")
    parser.add_argument("--tools-admin", action="store_const", const="admin", default=None, help="Enable admin tools")

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

    tools_groups = {
        "common": [
            # list_dir
            ListDir(),
            Search(),
            # get_attributes
            GetAttributes(),
            # check_is_paths_exists
            CheckIsPathsExists(),
            # common_client
            CommonCypress(),
        ],
        "account": [
            # account
            CheckPermissions(),
            AccountProperty(),
        ],
        "admin": [
            # admin
            GetProxy(),
        ],
    }

    tools = []
    for group in [app_args.tools_common, app_args.tools_account, app_args.tools_admin]:
        if group:
            tools.extend(tools_groups[group])
    if not tools:
        tools.extend(list(itertools.chain(*tools_groups.values())))

    mcp_runner.attach_tools(tools)

    if app_args.show_tools:
        print("Tools:")
        for tool in mcp_runner._tools:
            print(f"- {tool._get_tool_description()[0].name} ({tool.__class__.__name__})")
        exit()

    mcp_runner.start(
        transport=app_args.server_transport,
    )


if __name__ == "__main__":
    main()
