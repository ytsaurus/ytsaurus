#include "companion_manager.h"

#include "companion_client_detail.h"
#include "companion_entrypoint.h"
#include "companion_process_manager.h"
#include "companion_singleton_state.h"
#include "config.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void TCompanionManagerParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("timeout", &TThis::Timeout)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("backoff", &TThis::Backoff)
        .Default(TExponentialBackoffOptions{
            .InvocationCount = 30,
            .MinBackoff = TDuration::MilliSeconds(500),
            .MaxBackoff = TDuration::Seconds(10),
        });
    registrar.Parameter("run_process", &TThis::RunProcess)
        .Default(true);
    registrar.Parameter("entrypoint", &TThis::Entrypoint)
        .DefaultCtor([] {
            return New<TCompanionEntrypoint>();
        });
    registrar.Parameter("init_backoff", &TThis::InitBackoff)
        .Default(TExponentialBackoffOptions{
            .InvocationCount = std::numeric_limits<decltype(TThis::InitBackoff.InvocationCount)>::max(),
            .MinBackoff = TDuration::Seconds(5),
            .MaxBackoff = TDuration::Seconds(30),
        });
    registrar.Parameter("health_check_interval", &TThis::HealthCheckInterval)
        .Default(TDuration::Seconds(20));
    registrar.Parameter("startup_grace_period", &TThis::StartupGracePeriod)
        .Default(TDuration::Seconds(60));
    registrar.Parameter("metrics_collection_interval", &TThis::MetricsCollectionInterval)
        .Default(TDuration::Seconds(10));
    registrar.Parameter("restart_delay", &TThis::RestartDelay)
        .Default(TDuration::MilliSeconds(100));
}

////////////////////////////////////////////////////////////////////////////////

TCompanionManager::TCompanionManager(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : TResourceBase(context, std::move(dynamicContext))
    , CompanionConfig_(GetCompanionExecutionConfig())
    , CompanionAddress_(Format("0.0.0.0:%v", CompanionConfig_->Port))
    , CompanionClient_(CreateCompanionClient(GetContext()->StatusProfiler->WithPrefix("/common_companion_client")))
    , Profiler_(context->Profiler.WithPrefix("/companion_manager"))
{ }

ICompanionClientPtr TCompanionManager::CreateCompanionClient(IStatusProfilerPtr statusProfiler)
{
    return New<TCompanionClient>(
        CompanionAddress_,
        GetParameters()->Timeout,
        GetParameters()->Backoff,
        statusProfiler);
}

TProcessManagerBasePtr TCompanionManager::CreateProcessManager()
{
    return New<TCompanionProcessManager>(
        GetContext()->Invoker,
        CompanionClient_,
        GetParameters()->InitBackoff,
        GetParameters()->RestartDelay,
        GetParameters()->HealthCheckInterval,
        GetParameters()->StartupGracePeriod,
        GetParameters()->MetricsCollectionInterval,
        Logger,
        Profiler_,
        GetContext()->StatusProfiler->WithPrefix("/companion_process_manager"),
        GetParameters()->Entrypoint);
}

TFuture<void> TCompanionManager::Load(const THashMap<TResourceId, IResourcePtr>& /*dependencies*/)
{
    if (!GetParameters()->RunProcess) {
        YT_TLOG_INFO("Companion process running is disabled");
        return OKFuture;
    }
    ProcessManager_ = CreateProcessManager();
    return BIND([this, weakThis = MakeWeak(this)] () {
        if (auto strongThis = weakThis.Lock()) {
            ProcessManager_->Start();
        }
    })
        .AsyncVia(GetContext()->Invoker)
        .Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
