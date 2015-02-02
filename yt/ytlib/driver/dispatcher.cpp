#include "dispatcher.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace NDriver {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl()
        : LightPoolSize_(1)
        , LightPool_(BIND(
            NYT::New<TThreadPool, const int&, const Stroka&>,
            ConstRef(LightPoolSize_),
            "DriverLight"))
        , HeavyPoolSize_(4)
        , HeavyPool_(BIND(
            NYT::New<TThreadPool, const int&, const Stroka&>,
            ConstRef(HeavyPoolSize_),
            "DriverHeavy"))
    { }

    void Configure(int lightPoolSize, int heavyPoolSize)
    {
        if (LightPoolSize_ == lightPoolSize && HeavyPoolSize_ == heavyPoolSize) {
            return;
        }

        // We believe in proper memory ordering here.
        YCHECK(!LightPool_.HasValue());
        YCHECK(!HeavyPool_.HasValue());
        LightPoolSize_ = lightPoolSize;
        HeavyPoolSize_ = heavyPoolSize;
        // This is not redundant, since the check and the assignment above are
        // not atomic and (adversary) thread can initialize thread pool in parallel.
        YCHECK(!LightPool_.HasValue());
        YCHECK(!HeavyPool_.HasValue());
    }

    IInvokerPtr GetLightInvoker()
    {
        return LightPool_->GetInvoker();
    }

    IInvokerPtr GetHeavyInvoker()
    {
        return HeavyPool_->GetInvoker();
    }

    void Shutdown()
    {
        if (LightPool_.HasValue()) {
            LightPool_->Shutdown();
        }

        if (HeavyPool_.HasValue()) {
            HeavyPool_->Shutdown();
        }
    }

private:
    int LightPoolSize_;
    TLazyIntrusivePtr<NConcurrency::TThreadPool> LightPool_;

    int HeavyPoolSize_;
    TLazyIntrusivePtr<NConcurrency::TThreadPool> HeavyPool_;
};

TDispatcher::TDispatcher()
    : Impl_(new TImpl())
{ }

TDispatcher::~TDispatcher()
{ }

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::Configure(int lightPoolSize, int heavyPoolSize)
{
    Impl_->Configure(lightPoolSize, heavyPoolSize);
}

void TDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

IInvokerPtr TDispatcher::GetLightInvoker()
{
    return Impl_->GetLightInvoker();
}

IInvokerPtr TDispatcher::GetHeavyInvoker()
{
    return Impl_->GetHeavyInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
