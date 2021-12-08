#include "private.h"

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/lazy_ptr.h>

namespace NYT::NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const TString SnapshotExtension("snapshot");
const TString ChangelogExtension("log");
const TString ChangelogIndexExtension("index");
const NProfiling::TProfiler HydraProfiler("/hydra");

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
