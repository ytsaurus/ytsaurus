#include "config.h"
#include "dispatcher.h"

namespace NYT {
namespace NDriver {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TDispatcher::TDispatcher()
    : HeavyPoolSize(4)
    , DriverThread(
        TActionQueue::CreateFactory("Driver"))
    , HeavyThreadPool(BIND(
        NYT::New<TThreadPool, const int&, const Stroka&>,
        ConstRef(HeavyPoolSize),
        "DriverHeavy"))
    , Initialized(false)
{ }

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::Configure(TDriverConfigPtr config)
{
    if (Initialized) {
        YCHECK(HeavyPoolSize == config->HeavyPoolSize);
        return;
    }
    // We believe in proper memory ordering here.
    YCHECK(!HeavyThreadPool.HasValue());
    // We do not really want to store entire config within us.
    HeavyPoolSize = config->HeavyPoolSize;
    // This is not redundant, since the check and the assignment above are
    // not atomic and (adversary) thread can initialize thread pool in parallel.
    YCHECK(!HeavyThreadPool.HasValue());

    Initialized = true;
}

IInvokerPtr TDispatcher::GetLightInvoker()
{
    return DriverThread->GetInvoker();
}

IInvokerPtr TDispatcher::GetHeavyInvoker()
{
    return HeavyThreadPool->GetInvoker();
}

void TDispatcher::Shutdown()
{
    if (DriverThread.HasValue()) {
        DriverThread->Shutdown();
    }

    if (HeavyThreadPool.HasValue()) {
        HeavyThreadPool->Shutdown();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
