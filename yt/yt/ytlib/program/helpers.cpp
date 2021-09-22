#include "helpers.h"
#include "config.h"

#include <yt/yt/ytlib/chunk_client/dispatcher.h>

#include <yt/yt/core/ytalloc/bindings.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/library/tracing/jaeger/tracer.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/concurrency/execution_stack.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/concurrency/private.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/dispatcher.h>

#include <yt/yt/core/service_discovery/yp/service_discovery.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

namespace {

void SpinlockHiccupHandler(const ::TSourceLocation& location, ESpinlockActivityKind activityKind, i64 elapsedTicks)
{
    const auto& Logger = NConcurrency::ConcurrencyLogger;
    YT_LOG_DEBUG("Spinlock acquisition took too long (SourceLocation: %v:%v, ActivityKind: %v, Elapsed: %v)",
        location.File,
        location.Line,
        activityKind,
        NProfiling::CpuDurationToDuration(elapsedTicks));
}

} // namespace

void ConfigureSingletons(const TSingletonsConfigPtr& config)
{
    NConcurrency::SetSpinlockHiccupThresholdTicks(NProfiling::DurationToCpuDuration(config->SpinlockHiccupThreshold));
    NConcurrency::SetSpinlockHiccupHandler(&SpinlockHiccupHandler);

    if (!NYTAlloc::ConfigureFromEnv()) {
        NYTAlloc::Configure(config->YTAlloc);
    }

    for (const auto& [kind, size] : config->FiberStackPoolSizes) {
        NConcurrency::SetFiberStackPoolSize(ParseEnum<NConcurrency::EExecutionStackKind>(kind), size);
    }

    NLogging::TLogManager::Get()->EnableReopenOnSighup();
    if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
        NLogging::TLogManager::Get()->Configure(config->Logging);
    }

    NNet::TAddressResolver::Get()->Configure(config->AddressResolver);
    // By default, server component must have reasonable fqdn.
    // Failure to do so may result in issues like YT-4561.
    NNet::TAddressResolver::Get()->EnsureLocalHostName();

    NBus::TTcpDispatcher::Get()->Configure(config->TcpDispatcher);

    NRpc::TDispatcher::Get()->Configure(config->RpcDispatcher);

    NRpc::TDispatcher::Get()->SetServiceDiscovery(
        NServiceDiscovery::NYP::CreateServiceDiscovery(config->YPServiceDiscovery));

    NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);

    NTracing::SetGlobalTracer(New<NTracing::TJaegerTracer>(config->Jaeger));

    NProfiling::TProfileManager::Get()->Configure(config->ProfileManager);
    NProfiling::TProfileManager::Get()->Start();

    if (auto tracingConfig = config->Rpc->Tracing) {
        NTracing::SetTracingConfig(tracingConfig);
    }
}

void ReconfigureSingletons(const TSingletonsConfigPtr& config, const TSingletonsDynamicConfigPtr& dynamicConfig)
{
    NConcurrency::SetSpinlockHiccupThresholdTicks(NProfiling::DurationToCpuDuration(
        dynamicConfig->SpinlockHiccupThreshold.value_or(config->SpinlockHiccupThreshold)));

    if (!NYTAlloc::IsConfiguredFromEnv()) {
        NYTAlloc::Configure(dynamicConfig->YTAlloc ? dynamicConfig->YTAlloc : config->YTAlloc);
    }

    if (!NLogging::TLogManager::Get()->IsConfiguredFromEnv()) {
        NLogging::TLogManager::Get()->Configure(config->Logging->ApplyDynamic(dynamicConfig->Logging));
    }

    auto tracer = NTracing::GetGlobalTracer();
    if (auto jaeger = DynamicPointerCast<NTracing::TJaegerTracer>(tracer); jaeger) {
        jaeger->Configure(config->Jaeger->ApplyDynamic(dynamicConfig->Jaeger));
    }

    NBus::TTcpDispatcher::Get()->Configure(config->TcpDispatcher->ApplyDynamic(dynamicConfig->TcpDispatcher));

    NRpc::TDispatcher::Get()->Configure(config->RpcDispatcher->ApplyDynamic(dynamicConfig->RpcDispatcher));

    NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher->ApplyDynamic(dynamicConfig->ChunkClientDispatcher));

    NProfiling::TProfileManager::Get()->Reconfigure(config->ProfileManager, dynamicConfig->ProfileManager);

    if (dynamicConfig->Rpc->Tracing) {
        NTracing::SetTracingConfig(dynamicConfig->Rpc->Tracing);
    } else if (config->Rpc->Tracing) {
        NTracing::SetTracingConfig(config->Rpc->Tracing);
    }
}

void StartDiagnosticDump(const TDiagnosticDumpConfigPtr& config)
{
    static NLogging::TLogger Logger("DiagDump");

    static TPeriodicExecutorPtr YTAllocPeriodicExecutor;
    if (!YTAllocPeriodicExecutor && config->YTAllocDumpPeriod) {
        YTAllocPeriodicExecutor = New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            BIND([&] {
                YT_LOG_DEBUG("YTAlloc dump:\n%v",
                    NYTAlloc::FormatAllocationCounters());
            }),
            config->YTAllocDumpPeriod);
        YTAllocPeriodicExecutor->Start();
    }

    static TPeriodicExecutorPtr RefCountedTrackerPeriodicExecutor;
    if (!RefCountedTrackerPeriodicExecutor && config->RefCountedTrackerDumpPeriod) {
        RefCountedTrackerPeriodicExecutor = New<TPeriodicExecutor>(
            NRpc::TDispatcher::Get()->GetHeavyInvoker(),
            BIND([&] {
                YT_LOG_DEBUG("RefCountedTracker dump:\n%v",
                    TRefCountedTracker::Get()->GetDebugInfo());
            }),
            config->RefCountedTrackerDumpPeriod);
        RefCountedTrackerPeriodicExecutor->Start();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
