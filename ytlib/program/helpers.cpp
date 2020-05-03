#include "helpers.h"
#include "config.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/misc/ref_counted_tracker.h>

#include <yt/core/ytalloc/bindings.h>

#include <yt/core/tracing/trace_manager.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/concurrency/execution_stack.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/net/address.h>
#include <yt/core/net/local_address.h>

#include <yt/core/rpc/dispatcher.h>

namespace NYT {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

void ConfigureSingletons(const TSingletonsConfigPtr& config)
{
    for (const auto& [kind, size] : config->FiberStackPoolSizes) {
        NConcurrency::SetFiberStackPoolSize(ParseEnum<NConcurrency::EExecutionStackKind>(kind), size);
    }

    NLogging::TLogManager::Get()->Configure(config->Logging);

    NNet::TAddressResolver::Get()->Configure(config->AddressResolver);
    if (!NNet::TAddressResolver::Get()->IsLocalHostNameOK()) {
        THROW_ERROR_EXCEPTION("Could not determine local host FQDN");
    }

    NRpc::TDispatcher::Get()->Configure(config->RpcDispatcher);

    NChunkClient::TDispatcher::Get()->Configure(config->ChunkClientDispatcher);

    NTracing::TTraceManager::Get()->Configure(config->Tracing);

    NProfiling::TProfileManager::Get()->Configure(config->ProfileManager);
    NProfiling::TProfileManager::Get()->Start();
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
