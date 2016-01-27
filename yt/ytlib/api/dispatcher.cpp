#include "dispatcher.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/lazy_ptr.h>

namespace NYT {
namespace NApi {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl(int lightPoolSize, int heavyPoolSize)
        : LightPool_(BIND(
            New<TThreadPool, const int&, const Stroka&>,
            lightPoolSize,
            "ClientLight"))
        , HeavyPool_(BIND(
            New<TThreadPool, const int&, const Stroka&>,
            heavyPoolSize,
            "ClientHeavy"))
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
    TLazyIntrusivePtr<NConcurrency::TThreadPool> LightPool_;
    TLazyIntrusivePtr<NConcurrency::TThreadPool> HeavyPool_;
};

TDispatcher::TDispatcher(int lightPoolSize, int heavyPoolSize)
    : Impl_(new TImpl(lightPoolSize, heavyPoolSize))
{ }

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
