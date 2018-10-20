#include <yt/server/skynet_manager/bootstrap.h>
#include <yt/server/skynet_manager/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/phdr_cache/phdr_cache.h>

#include <yt/core/alloc/alloc.h>

namespace NYT {
namespace NSkynetManager {

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
        TThread::CurrentThreadSetName("SkynetManager");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        EnablePhdrCache();
        NYTAlloc::EnableLogging();
        NYTAlloc::EnableProfiling();
        NYTAlloc::EnableStockpile();

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

} // namespace NSkynetManager
} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::NSkynetManager::TSkynetManagerProgram().Run(argc, argv);
}

