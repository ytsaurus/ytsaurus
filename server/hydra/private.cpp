#include "private.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/lazy_ptr.h>
#include <yt/core/misc/shutdown.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const TString SnapshotExtension("snapshot");
const TString ChangelogExtension("log");
const TString ChangelogIndexExtension("index");

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

REGISTER_SHUTDOWN_CALLBACK(11, ShutdownHydraIOInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
