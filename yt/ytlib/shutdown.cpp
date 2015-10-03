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
    NPipes::TIODispatcher::StaticShutdown();
    NDriver::TDispatcher::StaticShutdown();
    NChunkClient::TDispatcher::StaticShutdown();
    NRpc::TDispatcher::StaticShutdown();
    NBus::TTcpDispatcher::StaticShutdown();
    NConcurrency::TDelayedExecutor::StaticShutdown();
    TAddressResolver::StaticShutdown();
    NLogging::TLogManager::StaticShutdown();
    NTracing::TTraceManager::StaticShutdown();
    NProfiling::TProfileManager::StaticShutdown();
    ShutdownFinalizerThread();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
