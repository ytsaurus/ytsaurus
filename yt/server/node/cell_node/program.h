#include <yt/server/node/cell_node/bootstrap.h>
#include <yt/server/node/cell_node/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_tool_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/program_setsid_mixin.h>
#include <yt/ytlib/program/program_cgroup_mixin.h>
#include <yt/ytlib/program/helpers.h>

#include <yt/library/phdr_cache/phdr_cache.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <yt/core/ytalloc/bindings.h>

#include <yt/core/misc/ref_counted_tracker_profiler.h>

namespace NYT::NCellNode {

////////////////////////////////////////////////////////////////////////////////

class TCellNodeProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramCgroupMixin
    , public TProgramToolMixin
    , public TProgramConfigMixin<NCellNode::TCellNodeConfig>
{
public:
    TCellNodeProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramCgroupMixin(Opts_)
        , TProgramToolMixin(Opts_)
        , TProgramConfigMixin(Opts_, false)
    {
        Opts_
            .AddLongOption("validate-snapshot")
            .StoreMappedResult(&ValidateSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("NodeMain");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        EnableRefCountedTrackerProfiling();
        NYTAlloc::EnableYTLogging();
        NYTAlloc::EnableYTProfiling();
        NYTAlloc::SetLibunwindBacktraceProvider();
        NYTAlloc::ConfigureFromEnv();
        NYTAlloc::EnableStockpile();
        NYTAlloc::MlockallCurrentProcess();

        if (HandleSetsidOptions()) {
            return;
        }
        if (HandleCgroupOptions()) {
            return;
        }
        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleToolOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();
        auto configNode = GetConfigNode();

        ConfigureSingletons(config);
        StartDiagnosticDump(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NCellNode::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Initialize();

        if (ValidateSnapshot_) {
            bootstrap->ValidateSnapshot(ValidateSnapshot_);
        } else {
            bootstrap->Run();
        }
    }

private:
    TString ValidateSnapshot_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellNode
