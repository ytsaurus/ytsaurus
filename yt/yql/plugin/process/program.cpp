#include "config.h"
#include "plugin_service.h"

#include <yt/yql/plugin/bridge/plugin.h>
#include <yt/yql/plugin/config.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/library/profiling/perf/event_counter_profiler.h>
#include <yt/yt/library/program/helpers.h>
#include <yt/yt/library/program/program.h>
#include <yt/yt/library/program/program_config_mixin.h>
#include <yt/yt/library/program/program_pdeathsig_mixin.h>
#include <yt/yt/library/program/program_setsid_mixin.h>

#include <library/cpp/yt/logging/backends/arcadia/backend.h>
#include <library/cpp/yt/mlock/mlock.h>

#include <util/system/thread.h>

namespace NYT::NYqlPlugin::NProcess {

////////////////////////////////////////////////////////////////////////////////

class TYqlPluginProgram
    : public virtual TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TProcessYqlPluginInternalConfig>
{
public:
    TYqlPluginProgram()
        : TProgram()
        , TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    { }

protected:
    void DoRun() override
    {
        TThread::SetCurrentThreadName("YqlPluginMain");

        ConfigureUids();
        ConfigureIgnoreSigpipe();
        ConfigureCrashHandler();
        ConfigureExitZeroOnSigterm();
        // We intentionally omit EnablePhdrCache() because not only YQL is loaded as a shared library, but also
        // YQL UDFs, and they may be user-provided in runtime for particular query.
        ConfigureAllocator({});
        MlockFileMappings();
        RunMixinCallbacks();

        auto config = GetConfig();

        ConfigureSingletons(config);

        NProfiling::EnablePerfEventCounterProfiling();

        auto controlQueue_ = New<NConcurrency::TActionQueue>("YqlPluginServiceControl");
        auto controlInvoker_ = controlQueue_->GetInvoker();

        YT_VERIFY(config->BusServer->UnixDomainSocketPath);

        config->PluginConfig->ProcessPluginConfig->RuntimeConfig->ApplyLimitations();

        auto options = ConvertToOptions(
            config->PluginConfig,
            NYson::ConvertToYsonString(config->SingletonsConfig),
            NLogging::CreateArcadiaLogBackend(NLogging::TLogger("YqlPlugin")),
            config->MaxSupportedYqlVersion,
            false);

        auto yqlPlugin = CreateBridgeYqlPlugin(std::move(options));
        yqlPlugin->Start();

        if (config->DynamicGatewaysConfig) {
            yqlPlugin->OnDynamicConfigChanged(TYqlPluginDynamicConfig{
                .GatewaysConfig = *config->DynamicGatewaysConfig,
                .MaxSupportedYqlVersion = NYson::TYsonString(config->MaxSupportedYqlVersion),
            });
        }

        auto yqlPluginService = CreateYqlPluginService(controlInvoker_, std::move(yqlPlugin));
        auto rpcServer = NRpc::NBus::CreateBusServer(NBus::CreateBusServer(config->BusServer));

        rpcServer->RegisterService(yqlPluginService);

        rpcServer->Configure(config->RpcServer);
        rpcServer->Start();

        Sleep(TDuration::Max());
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunYqlPluginProgram(int argc, const char** argv)
{
    TYqlPluginProgram().Run(argc, argv);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NProcess
