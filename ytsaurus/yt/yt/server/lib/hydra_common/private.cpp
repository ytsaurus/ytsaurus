#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static TActionQueuePtr GetHydraIOActionQueue()
{
    static const auto queue = New<TActionQueue>("HydraIO");
    return queue;
}

IInvokerPtr GetHydraIOInvoker()
{
    return GetHydraIOActionQueue()->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
