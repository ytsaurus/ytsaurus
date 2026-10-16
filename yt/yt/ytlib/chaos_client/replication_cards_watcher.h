#pragma once

#include "object_watcher.h"
#include "public.h"

#include <yt/yt/client/chaos_client/replication_card.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IReplicationCardWatcherCallbacks
    : public IObjectWatcherCallbacks<TReplicationCardPtr>
{ };

DEFINE_REFCOUNTED_TYPE(IReplicationCardWatcherCallbacks)

struct IReplicationCardsWatcher
    : public IObjectWatcher<TReplicationCardPtr>
{ };

DEFINE_REFCOUNTED_TYPE(IReplicationCardsWatcher)

IReplicationCardsWatcherPtr CreateReplicationCardsWatcher(
    TReplicationCardsWatcherConfigPtr config,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
