#include <yt/server/skynet_manager/bootstrap.h>
#include <yt/server/skynet_manager/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/phdr_cache/phdr_cache.h>

#include <library/ytalloc/api/ytalloc.h>

#include <yt/core/ytalloc/bindings.h>

#include <yt/core/misc/ref_counted_tracker_profiler.h>

namespace NYT::NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

class TSkynetManagerProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<TSkynetManagerConfig>
{
public:
    TSkynetManagerProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_, false)
    { }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("SkynetManager");

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

        if (HandlePdeathsigOptions()) {
            return;
        }

        if (HandleConfigOptions()) {
            return;
        }

        auto config = GetConfig();
        for (auto cluster : config->Clusters) {
            cluster->LoadToken();
        }

        ConfigureSingletons(config);

        auto bootstrap = New<TBootstrap>(std::move(config));
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkynetManager

int main(int argc, const char** argv)
{
    return NYT::NSkynetManager::TSkynetManagerProgram().Run(argc, argv);
}

