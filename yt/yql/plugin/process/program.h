#pragma once

#include "config.h"
#include "plugin_service.h"

#include <yt/yql/plugin/config.h>
#include <yt/yql/plugin/bridge/plugin.h>

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

namespace NYT::NYqlPlugin {
namespace NProcess {

class TYqlPluginProgram
    : public virtual TProgram
    , public TProgramPdeathsigMixin
    , public TProgramSetsidMixin
    , public TProgramConfigMixin<TYqlPluginProcessInternalConfig>
{
public:
    TYqlPluginProgram()
        : TProgram()
        , TProgramPdeathsigMixin(Opts_)
        , TProgramSetsidMixin(Opts_)
        , TProgramConfigMixin(Opts_)
    {
    }

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

        auto ControlQueue_ = New<NConcurrency::TActionQueue>("YqlPluginServiceControl");
        auto ControlInvoker_ = ControlQueue_->GetInvoker();

        YT_VERIFY(config->BusServer->UnixDomainSocketPath);

        auto options = ConvertToOptions(
            config->PluginConfig,
            NYson::ConvertToYsonString(ConvertTo<TSingletonsConfigPtr>(config)),
            NLogging::CreateArcadiaLogBackend(NLogging::TLogger("YqlPlugin")), 
            config->MaxSupportedYqlVersion,
            config->StartDqManager
        );

        auto yqlPlugin = CreateBridgeYqlPlugin(std::move(options));
        // yqlPlugin->Start();

        auto yqlPluginService = CreateYqlPluginService(ControlInvoker_, std::move(yqlPlugin));
        auto rpcServer = NRpc::NBus::CreateBusServer(NBus::CreateBusServer(config->BusServer));

        rpcServer->RegisterService(yqlPluginService);

        rpcServer->Configure(config->RpcServer);
        rpcServer->Start();

        Sleep(TDuration::Max());
    }
};

} // namespace NProcess
} // namespace NYT::NYqlPlugin
