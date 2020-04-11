#include <yt/server/clock_server/cluster_clock/bootstrap.h>
#include <yt/server/clock_server/cluster_clock/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/program_setsid_mixin.h>
#include <yt/ytlib/program/program_cgroup_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/logging/log_manager.h>
#include <yt/core/logging/config.h>

#include <yt/library/phdr_cache/phdr_cache.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <yt/core/ytalloc/bindings.h>

#include <yt/core/misc/ref_counted_tracker_profiler.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TClusterClockProgram
    : public TProgram
    , public TProgramCgroupMixin
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<NClusterClock::TClusterClockConfig>
{
public:
    TClusterClockProgram()
        : TProgramCgroupMixin(Opts_)
        , TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    {
        Opts_
            .AddLongOption("dump-snapshot", "dump master snapshot and exit")
            .StoreMappedResult(&DumpSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
        Opts_
            .AddLongOption("validate-snapshot", "validate master snapshot and exit")
            .StoreMappedResult(&ValidateSnapshot_, &CheckPathExistsArgMapper)
            .RequiredArgument("SNAPSHOT");
    }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("ClockMain");

        bool dumpSnapshot = parseResult.Has("dump-snapshot");
        bool validateSnapshot = parseResult.Has("validate-snapshot");

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
        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();
        auto configNode = GetConfigNode();

        if (dumpSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateSilent();
        } else if (validateSnapshot) {
            config->Logging = NLogging::TLogManagerConfig::CreateQuiet();
        }

        ConfigureSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NClusterClock::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Initialize();

        if (dumpSnapshot) {
            bootstrap->TryLoadSnapshot(DumpSnapshot_, true);
        } else if (validateSnapshot) {
            bootstrap->TryLoadSnapshot(ValidateSnapshot_, false);
        } else {
            bootstrap->Run();
        }
    }

private:
    TString DumpSnapshot_;
    TString ValidateSnapshot_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
