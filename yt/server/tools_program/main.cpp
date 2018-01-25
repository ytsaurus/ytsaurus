#include <yt/ytlib/program/program.h>

#include <yt/server/misc/configure_singletons.h>
#include <yt/core/tools/registry.h>
#include <yt/core/tools/tools.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/stracer.h>
#include <yt/core/misc/signaler.h>

#include <yt/ytlib/cgroup/cgroup.h>

namespace NYT {

REGISTER_TOOL(TStraceTool)
REGISTER_TOOL(TSignalerTool)
REGISTER_TOOL(TKillAllByUidTool)
REGISTER_TOOL(TRemoveDirAsRootTool)
REGISTER_TOOL(TRemoveDirContentAsRootTool)
REGISTER_TOOL(TMountTmpfsAsRootTool)
REGISTER_TOOL(TUmountAsRootTool)
REGISTER_TOOL(TSetThreadPriorityAsRootTool)
REGISTER_TOOL(TFSQuotaTool)
REGISTER_TOOL(TChownChmodTool)
REGISTER_TOOL(TExtractTarAsRootTool)
REGISTER_TOOL(TGetDirectorySizeAsRootTool)

namespace NCGroup {
REGISTER_TOOL(TKillProcessGroupTool)
} // namespace NCGroup

////////////////////////////////////////////////////////////////////////////////

class TToolsProgram
    : public TYTProgram
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
        TThread::CurrentThreadSetName("Tool");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        try {
            if (!ToolName_.empty()) {
                auto result = NTools::ExecuteTool(ToolName_, NYson::TYsonString(ToolSpec_));
                Cout << result.GetData();
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

int main(int argc, const char** argv)
{
    return NYT::TToolsProgram().Run(argc, argv);
}

