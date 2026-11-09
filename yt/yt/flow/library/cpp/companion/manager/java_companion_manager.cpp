#include "java_companion_manager.h"
#include "companion_entrypoint.h"
#include "java_process_manager.h"

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

void TJavaCompanionManagerParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("jdk_bin_path", &TThis::JdkBinPath)
        .Default("");
    registrar.Parameter("classpath", &TThis::Classpath)
        .Default("");
    registrar.Parameter("main_class", &TThis::MainClass)
        .Default("");
}

////////////////////////////////////////////////////////////////////////////////

TJavaCompanionManager::TJavaCompanionManager(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext)
    : TCompanionManager(std::move(context), std::move(dynamicContext))
{ }

TProcessManagerBasePtr TJavaCompanionManager::CreateProcessManager()
{
    THashMap<std::string, std::string> environment;
    if (const auto& entrypoint = GetParameters()->Entrypoint) {
        environment = entrypoint->Env;
    }
    return New<TJavaProcessManager>(
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
        GetParameters()->JdkBinPath,
        GetParameters()->Classpath,
        GetParameters()->MainClass,
        environment);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
