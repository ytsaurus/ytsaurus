#include "chaos_leases_watcher.h"

#include "config.h"
#include "private.h"

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

IChaosLeasesWatcherPtr CreateChaosLeasesWatcher(
    TChaosLeasesWatcherConfigPtr config,
    IInvokerPtr invoker)
{
    return New<TObjectWatcher<TChaosLeasePtr, IChaosLeasesWatcher>>(
        std::move(invoker),
        config->ExpirationSweepPeriod,
        config->PollExpirationTime,
        config->GoneLeasesExpirationTime,
        ChaosLeaseWatcherLogger(),
        "ChaosLease");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
