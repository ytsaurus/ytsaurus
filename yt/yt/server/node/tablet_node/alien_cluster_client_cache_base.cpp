#include "alien_cluster_client_cache_base.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

TAlienClusterClientCacheBase::TAlienClusterClientCacheBase(TDuration evictionPeriod)
    : EvictionPeriod_(evictionPeriod)
{ }

void TAlienClusterClientCacheBase::CheckAndRemoveExpired(TInstant now, bool force)
{
    if (force || now > EvictionPeriod_.ToDeadLine(LastExpirationCheck_)) {
        for (auto entryIt = CachedClients_.begin(); entryIt != CachedClients_.end();) {
            if (entryIt->second->GetConnection()->IsTerminated()) {
                CachedClients_.erase(entryIt++);
            } else {
                ++entryIt;
            }
        }

        LastExpirationCheck_ = now;
    }
}

TDuration TAlienClusterClientCacheBase::GetEvictionPeriod() const
{
    return EvictionPeriod_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
