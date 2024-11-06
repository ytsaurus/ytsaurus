#include "program.h"

#include "tools.h"

#include <library/cpp/yt/system/exit.h>

#include <util/system/thread.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

TToolsProgram::TToolsProgram()
{
    Opts_
        .AddLongOption("tool-name", "tool name to execute")
        .StoreResult(&ToolName_)
        .RequiredArgument("NAME");
    Opts_
        .AddLongOption("tool-spec", "tool specification")
        .StoreResult(&ToolSpec_)
        .RequiredArgument("SPEC");
}

void TToolsProgram::DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/)
{
    TThread::SetCurrentThreadName("Tool");

    ConfigureUids();
    ConfigureIgnoreSigpipe();
    ConfigureCrashHandler();

    try {
        auto result = NTools::ExecuteTool(ToolName_, NYson::TYsonString(ToolSpec_));
        Cout << result.AsStringBuf();
        Cout.Flush();
    } catch (const std::exception& ex) {
        AbortProcessDramatically(
            EProcessExitCode::GenericError,
            Format("Tool failed: %v", ex.what()));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
