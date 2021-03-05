#include "bootstrap.h"
#include "config.h"

#include <yt/yt/ytlib/program/program.h>
#include <yt/yt/ytlib/program/program_config_mixin.h>
#include <yt/yt/ytlib/program/program_pdeathsig_mixin.h>
#include <yt/yt/ytlib/program/program_setsid_mixin.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/library/phdr_cache/phdr_cache.h>

#include <library/cpp/ytalloc/api/ytalloc.h>

#include <yt/yt/core/ytalloc/bindings.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyProgram
    : public TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<NRpcProxy::TProxyConfig>
{
public:
    TRpcProxyProgram()
        : TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

protected:
    virtual void DoRun(const NLastGetopt::TOptsParseResult& parseResult) override
    {
        TThread::SetCurrentThreadName("ProxyMain");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        EnablePhdrCache();
        ConfigureExitZeroOnSigterm();
        NYTAlloc::EnableYTLogging();
        NYTAlloc::EnableYTProfiling();
        NYTAlloc::InitializeLibunwindInterop();
        NYTAlloc::SetEnableEagerMemoryRelease(false);
        NYTAlloc::EnableStockpile();
        NYTAlloc::MlockFileMappings();

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

        ConfigureSingletons(config);
        StartDiagnosticDump(config);

        // TODO(babenko): This memory leak is intentional.
        // We should avoid destroying bootstrap since some of the subsystems
        // may be holding a reference to it and continue running some actions in background threads.
        auto* bootstrap = new NRpcProxy::TBootstrap(std::move(config), std::move(configNode));
        bootstrap->Run();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
