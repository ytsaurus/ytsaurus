#include "dispatcher.h"

#include <core/misc/lazy_ptr.h>
#include <core/misc/singleton.h>

#include <core/concurrency/thread_pool.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    void Configure(int poolSize)
    {
        Pool_->Configure(poolSize);
    }

    IInvokerPtr GetInvoker()
    {
        return Pool_->GetInvoker();
    }

    void Shutdown()
    {
        Pool_->Shutdown();
    }

private:
    TThreadPoolPtr Pool_ = New<TThreadPool>(8, "Rpc");
};

TDispatcher::TDispatcher()
    : Impl_(new TImpl())
{ }

TDispatcher::~TDispatcher()
{ }

TDispatcher* TDispatcher::Get()
{
    return TSingletonWithFlag<TDispatcher>::Get();
}

void TDispatcher::StaticShutdown()
{
    if (TSingletonWithFlag<TDispatcher>::WasCreated()) {
        TSingletonWithFlag<TDispatcher>::Get()->Shutdown();
    }
}

void TDispatcher::Configure(int poolSize)
{
    Impl_->Configure(poolSize);
}

void TDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

IInvokerPtr TDispatcher::GetInvoker()
{
    return Impl_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
