#include "dispatcher.h"

#include <core/misc/lazy_ptr.h>
#include <core/misc/singleton.h>

#include <core/concurrency/thread_pool.h>

namespace NYT {
namespace NDriver {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    void Configure(int lightPoolSize, int heavyPoolSize)
    {
        LightPool_->Configure(lightPoolSize);
        HeavyPool_->Configure(heavyPoolSize);
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
        LightPool_->Shutdown();
        HeavyPool_->Shutdown();
    }

private:
    const TThreadPoolPtr LightPool_ = New<TThreadPool>(1, "DriverLight");
    const TThreadPoolPtr HeavyPool_ = New<TThreadPool>(1, "DriverHeavy");
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

void TDispatcher::StaticShutdown()
{
    return Get()->Shutdown();
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
