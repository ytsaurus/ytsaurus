from yt.mcp.lib.tools.list_dir import ListDir, Search
from yt.mcp.lib.tools.get_attributes import GetAttributes
from yt.mcp.lib.tools.check_is_paths_exists import CheckIsPathsExists
from yt.mcp.lib.tools.admin import GetProxy
from yt.mcp.lib.tools.account import CheckPermissions, AccountProperty
from yt.mcp.lib.tools.common_client import CommonCypress
from yt.mcp.lib.tools.table import ReadStaticTable
from yt.mcp.lib.tool_runner_mcp import YTToolRunnerMCP


def get_tools_groups():
    return {
        "common": [
            ListDir(),
            Search(),
            GetAttributes(),
            CheckIsPathsExists(),
            CommonCypress(),
            ReadStaticTable(),
        ],
        "account": [
            CheckPermissions(),
            AccountProperty(),
        ],
        "admin": [
            GetProxy(),
        ],
    }


def get_all_tool_names():
    """Return names of all tools that will be registered in MCP server"""
    runner = YTToolRunnerMCP()
    tools = []
    for group_tools in get_tools_groups().values():
        tools.extend(group_tools)
    runner.attach_tools(tools=tools, variants=None)
    return [tool._get_tool_description()[0].name for tool in runner._tools]
