#pragma once

#include "object_watcher.h"
#include "public.h"

#include <yt/yt/client/chaos_client/chaos_lease.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IChaosLeaseWatcherCallbacks
    : public IObjectWatcherCallbacks<TChaosLeasePtr>
{ };

DEFINE_REFCOUNTED_TYPE(IChaosLeaseWatcherCallbacks)

struct IChaosLeasesWatcher
    : public IObjectWatcher<TChaosLeasePtr>
{ };

DEFINE_REFCOUNTED_TYPE(IChaosLeasesWatcher)

IChaosLeasesWatcherPtr CreateChaosLeasesWatcher(
    TChaosLeasesWatcherConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
