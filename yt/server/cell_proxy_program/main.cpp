#include <yt/server/cell_proxy/bootstrap.h>
#include <yt/server/cell_proxy/config.h>

#include <yt/ytlib/program/program.h>
#include <yt/ytlib/program/program_config_mixin.h>
#include <yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/core/phdr_cache/phdr_cache.h>

#include <util/system/mlock.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TCellProxyProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramConfigMixin<NCellProxy::TCellProxyConfig>
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

        try {
            LockAllMemory(ELockAllMemoryFlag::LockCurrentMemory | ELockAllMemoryFlag::LockFutureMemory);
        } catch (const std::exception& ex) {
            OnError(Format("Failed to lock memory: %v", ex.what()));
        }

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
        auto* bootstrap = new NCellProxy::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char** argv)
{
    return NYT::TCellProxyProgram().Run(argc, argv);
}

