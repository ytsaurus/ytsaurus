#pragma once

#include <yt/yt/server/lib/exec_agent/slot_location_builder.h>

#include <yt/yt/ytlib/program/program.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/tools/registry.h>
#include <yt/yt/ytlib/tools/tools.h>
#include <yt/yt/ytlib/tools/proc.h>
#include <yt/yt/ytlib/tools/signaler.h>

#include <yt/yt/ytlib/cgroup/cgroup.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NTools {
REGISTER_TOOL(TSignalerTool)
REGISTER_TOOL(TReadProcessSmapsTool)
REGISTER_TOOL(TKillAllByUidTool)
REGISTER_TOOL(TRemoveDirAsRootTool)
REGISTER_TOOL(TRemoveDirContentAsRootTool)
REGISTER_TOOL(TMountTmpfsAsRootTool)
REGISTER_TOOL(TUmountAsRootTool)
REGISTER_TOOL(TSetThreadPriorityAsRootTool)
REGISTER_TOOL(TFSQuotaTool)
REGISTER_TOOL(TChownChmodTool)
REGISTER_TOOL(TCopyDirectoryContentTool)
REGISTER_TOOL(TGetDirectorySizeAsRootTool)
} // namespace NTools

namespace NCGroup {
REGISTER_TOOL(TKillProcessGroupTool)
} // namespace NCGroup

namespace NExecAgent {
REGISTER_TOOL(TSlotLocationBuilderTool)
} // namespace NExecAgent

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
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("Tool");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        try {
            if (!ToolName_.empty()) {
                auto result = NTools::ExecuteTool(ToolName_, NYson::TYsonString(ToolSpec_));
                Cout << result.AsStringBuf();
                Cout.Flush();
                return;
            }
        } catch (const std::exception& ex) {
            Cerr << ex.what();
            Cerr.Flush();
        }

        _exit(1);
    }

private:
    TString ToolName_;
    TString ToolSpec_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
