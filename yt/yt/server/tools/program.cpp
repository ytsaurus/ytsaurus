#include "program.h"

#include "tools.h"

#include <yt/yt/library/program/program.h>

#include <library/cpp/yt/system/exit.h>

#include <util/system/thread.h>

namespace NYT::NTools {

////////////////////////////////////////////////////////////////////////////////

class TToolsProgram
    : public TProgram
{
public:
    TToolsProgram()
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

protected:
    TString ToolName_;
    TString ToolSpec_;

    void DoRun() final
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
};

////////////////////////////////////////////////////////////////////////////////

void RunToolsProgram(int argc, const char** argv)
{
    TToolsProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTools
