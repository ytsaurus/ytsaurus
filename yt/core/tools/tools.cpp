#include "stdafx.h"

#include "tools.h"

#include "registry.h"

#include <core/misc/subprocess.h>

#include <core/ytree/fluent.h>

#include <core/logging/log.h>
#include <core/logging/log_manager.h>
#include <core/logging/config.h>

namespace NYT {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYsonString SpawnTool(const Stroka& toolName, const TYsonString& serializedArgument)
{
    auto process = TSubprocess::CreateCurrentProcessSpawner();
    process.AddArguments({
        "--tool",
        toolName,
        "--spec",
        serializedArgument.Data()
    });

    auto result = process.Execute();
    if (!result.Status.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to run %v", toolName)
            << result.Status
            << TErrorAttribute("command_line", process.GetCommandLine())
            << TErrorAttribute("error", Stroka(result.Error.Begin(), result.Error.End()));
    }

    auto serializedResultOrError = Stroka(result.Output.Begin(), result.Output.End());

    // Treat empty string as OK
    if (serializedResultOrError.Empty()) {
        return ConvertToYsonString(TError());
    }

    return TYsonString(serializedResultOrError);
}

TYsonString DoRunTool(const Stroka& toolName, const TYsonString& serializedArgument)
{
    return SpawnTool(toolName, serializedArgument);
}

TYsonString DoRunToolInProcess(const Stroka& toolName, const TYsonString& serializedArgument)
{
    auto serializedResultOrError = ExecuteTool(toolName, serializedArgument);

    // Treat empty string as OK
    if (serializedResultOrError.Data().Empty()) {
        return ConvertToYsonString(TError());
    }

    return serializedResultOrError;
}

void InitLogging()
{
    const char* const stderrWriterName = "stderr";

    auto rule = New<NLogging::TRuleConfig>();
    rule->Writers.push_back(stderrWriterName);
    rule->MinLevel = NLogging::ELogLevel::Info;

    auto config = New<NLogging::TLogConfig>();
    config->Rules.push_back(std::move(rule));

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 0;
    config->LowBacklogWatermark = 0;

    auto stderrWriter = New<NLogging::TWriterConfig>();
    stderrWriter->Type = NLogging::EWriterType::Stderr;

    config->WriterConfigs.insert(std::make_pair(stderrWriterName, std::move(stderrWriter)));

    NLogging::TLogManager::Get()->Configure(std::move(config));
}

TYsonString ExecuteTool(const Stroka& toolName, const TYsonString& serializedArgument)
{
    try {
        InitLogging();

        const auto* registry = GetToolRegistry();
        YCHECK(registry != nullptr);

        auto it = registry->find(toolName);
        if (it == registry->end()) {
            THROW_ERROR_EXCEPTION("Failed to execute %v: no such tool", toolName);
        }

        const auto& toolDescription = it->second;

        NConcurrency::SetCurrentThreadName(~toolDescription.Name);

        auto result = (toolDescription.Tool)(serializedArgument);
        return result;
    } catch (const TErrorException& ex) {
        return ConvertToYsonString(ex.Error());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT