#include "stdafx.h"
#include "rpc_dispatcher.h"

#include <util/generic/singleton.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static const int ThreadPoolSize = 8;

////////////////////////////////////////////////////////////////////////////////

TRpcDispatcher::TRpcDispatcher()
    : ThreadPool(New<TThreadPool>(ThreadPoolSize, "Rpc"))
{ }

TRpcDispatcher* TRpcDispatcher::Get()
{
    return Singleton<TRpcDispatcher>();
}

IInvokerPtr TRpcDispatcher::GetPoolInvoker()
{
    return ThreadPool->GetInvoker();
}

void TRpcDispatcher::Shutdown()
{
    ThreadPool->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////           

} // namespace NRpc
} // namespace NYT
