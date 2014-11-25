#include "shutdown.h"

#include <core/concurrency/fiber.h>

#include <core/profiling/profile_manager.h>

#include <core/misc/address.h>

#include <core/bus/tcp_dispatcher.h>

#include <core/rpc/dispatcher.h>

#include <core/logging/log_manager.h>

#include <core/tracing/trace_manager.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <ytlib/pipes/io_dispatcher.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void Shutdown()
{
    NPipes::TIODispatcher::Get()->Shutdown();
    NDriver::TDispatcher::Get()->Shutdown();
    NChunkClient::TDispatcher::Get()->Shutdown();
    NRpc::TDispatcher::Get()->Shutdown();
    NBus::TTcpDispatcher::Get()->Shutdown();
    NConcurrency::TDelayedExecutor::Shutdown();
    NProfiling::TProfileManager::Get()->Shutdown();
    TAddressResolver::Get()->Shutdown();
    NLog::TLogManager::Get()->Shutdown();
    NTracing::TTraceManager::Get()->Shutdown();
    NConcurrency::NDetail::ShutdownUnwindThread();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
