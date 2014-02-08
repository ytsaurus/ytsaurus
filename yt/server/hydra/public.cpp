#include "stdafx.h"
#include "public.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

IInvokerPtr GetHydraIOInvoker()
{
    static TLazyIntrusivePtr<TActionQueue> HydraIOQueue(
        TActionQueue::CreateFactory("HydraIO"));
    return HydraIOQueue->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
