#include "dispatcher.h"

#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/misc/lazy_ptr.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl(int lightPoolSize, int heavyPoolSize)
        : LightPool_(New<TThreadPool>(lightPoolSize, "ClientLight"))
        , HeavyPool_(New<TThreadPool>(heavyPoolSize, "ClientHeavy"))
    { }

    IInvokerPtr GetLightInvoker()
    {
        return LightPool_->GetInvoker();
    }

    IInvokerPtr GetHeavyInvoker()
    {
        return HeavyPool_->GetInvoker();
    }

private:
    TThreadPoolPtr LightPool_;
    TThreadPoolPtr HeavyPool_;
};

TDispatcher::TDispatcher(int lightPoolSize, int heavyPoolSize)
    : Impl_(new TImpl(lightPoolSize, heavyPoolSize))
{ }

TDispatcher::~TDispatcher() = default;

IInvokerPtr TDispatcher::GetLightInvoker()
{
    return Impl_->GetLightInvoker();
}

IInvokerPtr TDispatcher::GetHeavyInvoker()
{
    return Impl_->GetHeavyInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
