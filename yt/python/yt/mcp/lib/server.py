from yt.mcp.lib.tools.list_dir import ListDir, Search
from yt.mcp.lib.tools.get_attributes import GetAttributes
from yt.mcp.lib.tools.check_is_paths_exists import CheckIsPathsExists
from yt.mcp.lib.tools.admin import GetProxy
from yt.mcp.lib.tools.account import CheckPermissions, AccountProperty
from yt.mcp.lib.tools.common_client import CommonCypress
from yt.mcp.lib.tools.table import ReadStaticTable


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
