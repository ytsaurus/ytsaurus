#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

IReplicationCardCachePtr CreateNativeReplicationCardCache(
    TReplicationCardCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
