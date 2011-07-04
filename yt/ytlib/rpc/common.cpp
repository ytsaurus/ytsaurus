#include "common.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

TRpcManager::TRpcManager()
    : Logger("Rpc")
{ }

TRpcManager* TRpcManager::Get()
{
    return Singleton<TRpcManager>();
}

NLog::TLogger& TRpcManager::GetLogger()
{
    return Logger;
}

Stroka TRpcManager::GetDebugInfo()
{
    // TODO: implement
    return "";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

