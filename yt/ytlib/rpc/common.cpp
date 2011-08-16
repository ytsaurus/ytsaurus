#include "common.h"

// TODO: hack

namespace NYT {
namespace NBus {

extern void ShutdownClientDispatcher();

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
    return "";
}

void TRpcManager::Shutdown()
{
    NBus::ShutdownClientDispatcher();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

