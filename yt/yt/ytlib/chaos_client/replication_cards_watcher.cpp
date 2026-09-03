#include "replication_cards_watcher.h"

#include "config.h"
#include "private.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

IReplicationCardsWatcherPtr CreateReplicationCardsWatcher(
    TReplicationCardsWatcherConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TObjectWatcher<TReplicationCardPtr, IReplicationCardsWatcher>>(
        std::move(invoker),
        config->ExpirationSweepPeriod,
        config->PollExpirationTime,
        config->GoneCardsExpirationTime,
        ReplicationCardWatcherLogger(),
        "ReplicationCard");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
