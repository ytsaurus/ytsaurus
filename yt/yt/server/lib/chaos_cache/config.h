#pragma once

#include "public.h"

#include <yt/yt/ytlib/chaos_client/public.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NChaosCache {

////////////////////////////////////////////////////////////////////////////////

class TChaosCacheConfig
    : public TSlruCacheConfig
{
public:
    NChaosClient::TReplicationCardsWatcherConfigPtr ReplicationCardsWatcher;
    TDuration UnwatchedCardExpirationDelay;

    REGISTER_YSON_STRUCT(TChaosCacheConfig);

    static void Register(TRegistrar);
};

DEFINE_REFCOUNTED_TYPE(TChaosCacheConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosCache
