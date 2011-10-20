#include "stdafx.h"
#include "common.h"

// TODO: hack

namespace NYT {
namespace NBus {

extern void ShutdownClientDispatcher();
extern Stroka GetClientDispatcherDebugInfo();

}
}

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

NLog::TLogger RpcLogger("Rpc");

////////////////////////////////////////////////////////////////////////////////

TRpcManager::TRpcManager()
{ }

TRpcManager* TRpcManager::Get()
{
    return Singleton<TRpcManager>();
}

Stroka TRpcManager::GetDebugInfo()
{
    // TODO: implement
    return NBus::GetClientDispatcherDebugInfo();
}

void TRpcManager::Shutdown()
{
    NBus::ShutdownClientDispatcher();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

