#include "stdafx.h"
#include "dispatcher.h"

#include <core/misc/singleton.h>

#include <util/generic/singleton.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static const int ThreadPoolSize = 8;

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : ThreadPool(New<NConcurrency::TThreadPool>(ThreadPoolSize, "Rpc"))
{ }

TDispatcher::~TDispatcher()
{
    ThreadPool->Shutdown();
}

TDispatcher* TDispatcher::Get()
{
    return TSingleton::Get();
}

IInvokerPtr TDispatcher::GetPoolInvoker()
{
    return ThreadPool->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
