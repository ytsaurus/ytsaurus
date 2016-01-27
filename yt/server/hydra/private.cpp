#include "private.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/lazy_ptr.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const Stroka SnapshotExtension("snapshot");
const Stroka ChangelogExtension("log");
const Stroka ChangelogIndexExtension("index");

static TActionQueuePtr GetHydraIOActionQueue()
{
    static auto queue = New<TActionQueue>("HydraIO");
    return queue;
}

IInvokerPtr GetHydraIOInvoker()
{
    return GetHydraIOActionQueue()->GetInvoker();
}

void ShutdownHydraIOInvoker()
{
    GetHydraIOActionQueue()->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
