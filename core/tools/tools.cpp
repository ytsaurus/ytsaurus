#include "tools.h"
#include "registry.h"

#include <yt/core/logging/config.h>
#include <yt/core/logging/log.h>
#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/subprocess.h>

#include <yt/core/ytree/fluent.h>

namespace NYT {
namespace NTools {

////////////////////////////////////////////////////////////////////////////////

static const char* ToolsProgramName = "ytserver-tools";

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYsonString SpawnTool(const TString& toolName, const TYsonString& serializedArgument)
{
    auto process = TSubprocess(ToolsProgramName);
    process.AddArguments({
        "--tool-name",
        toolName,
        "--tool-spec",
        serializedArgument.GetData()
    });

    auto result = process.Execute();
    if (!result.Status.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to run %v", toolName)
            << result.Status
            << TErrorAttribute("command_line", process.GetCommandLine())
            << TErrorAttribute("error", TString(result.Error.Begin(), result.Error.End()));
    }

    auto serializedResultOrError = TString(result.Output.Begin(), result.Output.End());

    // Treat empty string as OK
    if (serializedResultOrError.Empty()) {
        return ConvertToYsonString(TError(), NYson::EYsonFormat::Text);
    }

    return TYsonString(serializedResultOrError);
}

TYsonString DoRunTool(const TString& toolName, const TYsonString& serializedArgument)
{
    return SpawnTool(toolName, serializedArgument);
}

TYsonString DoRunToolInProcess(const TString& toolName, const TYsonString& serializedArgument)
{
    auto serializedResultOrError = ExecuteTool(toolName, serializedArgument);

    // Treat empty string as OK
    if (serializedResultOrError.GetData().empty()) {
        return ConvertToYsonString(TError(), NYson::EYsonFormat::Text);
    }

    return serializedResultOrError;
}

TYsonString ExecuteTool(const TString& toolName, const TYsonString& serializedArgument)
{
    try {
        // No logging inside tools.
        NLogging::TLogManager::StaticShutdown();

        const auto* registry = GetToolRegistry();
        YCHECK(registry != nullptr);

        auto it = registry->find(toolName);
        if (it == registry->end()) {
            THROW_ERROR_EXCEPTION("Failed to execute %v: no such tool", toolName);
        }

        const auto& toolDescription = it->second;

        TThread::CurrentThreadSetName(toolDescription.Name.c_str());

        auto result = toolDescription.Tool(serializedArgument);
        return result;
    } catch (const TErrorException& ex) {
        return ConvertToYsonString(ex.Error(), NYson::EYsonFormat::Text);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTools
} // namespace NYT
