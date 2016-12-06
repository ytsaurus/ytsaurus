#include "shutdown.h"

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/core/actions/invoker_util.h>

#include <yt/core/bus/tcp_dispatcher.h>

#include <yt/core/concurrency/fiber.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/address.h>

#include <yt/core/pipes/io_dispatcher.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/dispatcher.h>

#include <yt/core/tracing/trace_manager.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void Shutdown()
{
    NPipes::TIODispatcher::StaticShutdown();
    NChunkClient::TDispatcher::StaticShutdown();
    NTracing::TTraceManager::StaticShutdown();
    NRpc::TDispatcher::StaticShutdown();
    NBus::TTcpDispatcher::StaticShutdown();
    NLogging::TLogManager::StaticShutdown();
    NProfiling::TProfileManager::StaticShutdown();
    NConcurrency::TDelayedExecutor::StaticShutdown();
    TAddressResolver::StaticShutdown();
    ShutdownFinalizerThread();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
