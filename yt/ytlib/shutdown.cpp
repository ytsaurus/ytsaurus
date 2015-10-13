#include "shutdown.h"

#include <core/concurrency/fiber.h>

#include <core/profiling/profile_manager.h>

#include <core/misc/address.h>

#include <core/actions/invoker_util.h>

#include <core/bus/tcp_dispatcher.h>

#include <core/rpc/dispatcher.h>

#include <core/logging/log_manager.h>

#include <core/tracing/trace_manager.h>

#include <core/profiling/profile_manager.h>

#include <core/pipes/io_dispatcher.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/chunk_client/dispatcher.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void Shutdown()
{
    NDriver::TDispatcher::StaticShutdown();
    NPipes::TIODispatcher::StaticShutdown();
    NChunkClient::TDispatcher::StaticShutdown();
    NRpc::TDispatcher::StaticShutdown();
    NBus::TTcpDispatcher::StaticShutdown();
    NTracing::TTraceManager::StaticShutdown();
    TAddressResolver::StaticShutdown();
    NLogging::TLogManager::StaticShutdown();
    NProfiling::TProfileManager::StaticShutdown();
    NConcurrency::TDelayedExecutor::StaticShutdown();
    ShutdownFinalizerThread();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
