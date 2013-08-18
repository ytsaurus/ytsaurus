#include "shutdown.h"

#include <ytlib/profiling/profiling_manager.h>
#include <ytlib/rpc/dispatcher.h>
#include <ytlib/bus/tcp_dispatcher.h>
#include <ytlib/chunk_client/dispatcher.h>
#include <ytlib/logging/log_manager.h>

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
    // TODO: refactor system shutdown
    // XXX(sandello): Keep in sync with server/main.cpp, driver/main.cpp and utmain.cpp, python_bindings/driver.cpp
    NBus::TTcpDispatcher::Get()->Shutdown();
    NRpc::TDispatcher::Get()->Shutdown();
    NChunkClient::TDispatcher::Get()->Shutdown();
    NProfiling::TProfilingManager::Get()->Shutdown();
    TDelayedInvoker::Shutdown();
    NLog::TLogManager::Get()->Shutdown();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NPython

} // namespace NYT

