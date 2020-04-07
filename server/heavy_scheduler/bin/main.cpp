#include <yp/server/heavy_scheduler/bootstrap.h>
#include <yp/server/heavy_scheduler/config.h>

#include <yt/ytlib/program/configure_singletons.h>
#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>

#include <yt/library/phdr_cache/phdr_cache.h>

#include <yt/core/misc/ref_counted_tracker_profiler.h>
#include <yt/core/ytalloc/bindings.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYP::NServer::NHeavyScheduler {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

class THeavySchedulerProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<NHeavyScheduler::THeavySchedulerProgramConfig>
{
public:
    THeavySchedulerProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& /*parseResult*/) override
    {
        TThread::SetCurrentThreadName("HeavySchedulerMain");

        ConfigureSignals();
        ConfigureCrashHandler();
        EnablePhdrCache();
        ConfigureExitZeroOnSigterm();
        EnableRefCountedTrackerProfiling();
        NYTAlloc::EnableYTLogging();
        NYTAlloc::EnableYTProfiling();
        NYTAlloc::SetLibunwindBacktraceProvider();
        NYTAlloc::ConfigureFromEnv();
        NYTAlloc::EnableStockpile();
        NYTAlloc::MlockallCurrentProcess();

        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();

        ConfigureSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NHeavyScheduler::TBootstrap(std::move(config));
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler

int main(int argc, const char** argv)
{
    return NYP::NServer::NHeavyScheduler::THeavySchedulerProgram().Run(argc, argv);
}
