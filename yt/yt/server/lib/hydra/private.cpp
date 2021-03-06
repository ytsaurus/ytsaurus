#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/shutdown.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const TString SnapshotExtension("snapshot");
const TString ChangelogExtension("log");
const TString ChangelogIndexExtension("index");
const NProfiling::TProfiler HydraProfiler("/hydra");

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

} // namespace NYT::NHydra
