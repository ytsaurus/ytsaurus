#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <util/datetime/base.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TAlienClusterClientCacheBase
{
public:
    explicit TAlienClusterClientCacheBase(TDuration evictionPeriod);

protected:
    THashMap<std::string, NApi::NNative::IClientPtr> CachedClients_;
    TInstant LastExpirationCheck_;

    void CheckAndRemoveExpired(TInstant now, bool force);
    TDuration GetEvictionPeriod() const;

private:
    const TDuration EvictionPeriod_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
