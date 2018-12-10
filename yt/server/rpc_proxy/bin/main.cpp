#include <yt/server/rpc_proxy/bootstrap.h>
#include <yt/server/rpc_proxy/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/phdr_cache/phdr_cache.h>

#include <yt/core/alloc/alloc.h>

namespace NYT::NCellProxy {

////////////////////////////////////////////////////////////////////////////////

class TCellProxyProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<NRpcProxy::TCellProxyConfig>
{
public:
    TCellProxyProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::CurrentThreadSetName("ProxyMain");

        ConfigureUids();
        ConfigureSignals();
        ConfigureCrashHandler();
        EnablePhdrCache();
        ConfigureExitZeroOnSigterm();
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
        auto configNode = GetConfigNode();

        ConfigureSingletons(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NRpcProxy::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellProxy

int main(int argc, const char** argv)
{
    return NYT::NCellProxy::TCellProxyProgram().Run(argc, argv);
}

