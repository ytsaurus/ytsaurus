#include "program_tool_mixin.h"

#include <yt/core/tools/tools.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TProgramToolMixin::TProgramToolMixin(NLastGetopt::TOpts& opts)
{
    opts
        .AddLongOption("tool-name", "tool name to execute")
        .StoreResult(&ToolName_)
        .RequiredArgument("NAME");
    opts
        .AddLongOption("tool-spec", "tool specification")
        .StoreResult(&ToolSpec_)
        .RequiredArgument("SPEC");
}

bool TProgramToolMixin::HandleToolOptions()
{
    if (!ToolName_.empty()) {
        auto result = NTools::ExecuteTool(ToolName_, NYson::TYsonString(ToolSpec_));
        Cout << result.GetData();
        Cout.Flush();
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
