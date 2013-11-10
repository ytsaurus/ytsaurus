#include "shutdown.h"

#include <core/misc/address.h>

#include <core/profiling/profiling_manager.h>

#include <core/rpc/dispatcher.h>

#include <core/bus/tcp_dispatcher.h>

#include <core/logging/log_manager.h>

#include <ytlib/chunk_client/dispatcher.h>

#include <contrib/libs/pycxx/Objects.hxx>

namespace NYT {
namespace NPython {

///////////////////////////////////////////////////////////////////////////////

void RegisterShutdown()
{
    static bool registered = false;
    if (!registered) {
        registered = true;
        Py_AtExit(Shutdown);
    }
}

void Shutdown()
{
    // TODO(sandello): Refactor this.
    // XXX(sandello): Keep in sync with...
    //   server/main.cpp
    //   driver/main.cpp
    //   unittests/utmain.cpp
    //   nodejs/src/common.cpp
    //   ../python/yt/bindings/shutdown.cpp
    // Feel free to add your cpp here. Welcome to the Shutdown Club!

    NBus::TTcpDispatcher::Get()->Shutdown();
    NRpc::TDispatcher::Get()->Shutdown();
    NChunkClient::TDispatcher::Get()->Shutdown();
    NProfiling::TProfilingManager::Get()->Shutdown();
    NConcurrency::TDelayedExecutor::Shutdown();
    TAddressResolver::Get()->Shutdown();
    NLog::TLogManager::Get()->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

