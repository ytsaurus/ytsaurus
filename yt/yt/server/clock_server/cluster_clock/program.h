#include <yt/yt/server/clock_server/cluster_clock/bootstrap.h>
#include <yt/yt/server/clock_server/cluster_clock/config.h>

#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <library/cpp/yt/phdr_cache/phdr_cache.h>

#include <library/cpp/yt/mlock/mlock.h>

#include <yt/yt/core/misc/ref_counted_tracker_profiler.h>

#include <util/system/compiler.h>
#include <util/system/thread.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

class TClusterClockProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<NClusterClock::TClusterClockConfig>
{
public:
    TClusterClockProgram()
        : TProgramPdeathsigMixin(Opts_)
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
    void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("ClockMain");

        bool dumpSnapshot = parseResult.Has("dump-snapshot");
        bool validateSnapshot = parseResult.Has("validate-snapshot");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        ConfigureAllocator();

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
        StartDiagnosticDump(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NClusterClock::TBootstrap(std::move(config), std::move(configNode));
        DoNotOptimizeAway(bootstrap);
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

} // namespace NYT::NClusterClock
