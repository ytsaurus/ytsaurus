#include "config.h"
#include "dispatcher.h"

#include <core/misc/singleton.h>

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
{ }

TDispatcher::~TDispatcher()
{
    if (DriverThread.HasValue()) {
        DriverThread->Shutdown();
    }

    if (HeavyThreadPool.HasValue()) {
        HeavyThreadPool->Shutdown();
    }
}

TDispatcher* TDispatcher::Get()
{
    return TSingleton::Get();
}

void TDispatcher::Configure(int heavyPoolSize)
{
    if (HeavyPoolSize == heavyPoolSize) {
        return;
    }

    // We believe in proper memory ordering here.
    YCHECK(!HeavyThreadPool.HasValue());
    // We do not really want to store entire config within us.
    HeavyPoolSize = heavyPoolSize;
    // This is not redundant, since the check and the assignment above are
    // not atomic and (adversary) thread can initialize thread pool in parallel.
    YCHECK(!HeavyThreadPool.HasValue());
}

IInvokerPtr TDispatcher::GetLightInvoker()
{
    return DriverThread->GetInvoker();
}

IInvokerPtr TDispatcher::GetHeavyInvoker()
{
    return HeavyThreadPool->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
