#include "stdafx.h"
#include "dispatcher.h"

#include <util/generic/singleton.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static const int ThreadPoolSize = 8;

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : ThreadPool(New<NConcurrency::TThreadPool>(ThreadPoolSize, "Rpc"))
{ }

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

IInvokerPtr TDispatcher::GetPoolInvoker()
{
    return ThreadPool->GetInvoker();
}

void TDispatcher::Shutdown()
{
    if (ThreadPool) {
        ThreadPool->Shutdown();
        ThreadPool.Reset();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
