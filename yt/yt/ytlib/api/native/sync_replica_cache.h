#pragma once

#include "public.h"

#include <yt/yt/client/tablet_client/public.h>
#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/async_expiring_cache.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

using TSyncReplicaCacheKey = NYPath::TYPath;

////////////////////////////////////////////////////////////////////////////////

class TSyncReplicaCache
    : public TAsyncExpiringCache<TSyncReplicaCacheKey, TTableReplicaInfoPtrList>
{
public:
    TSyncReplicaCache(
        TAsyncExpiringCacheConfigPtr config,
        IConnectionPtr connection,
        const NLogging::TLogger& logger);

protected:
    TFuture<TTableReplicaInfoPtrList> DoGet(
        const TSyncReplicaCacheKey& key,
        bool /*isPeriodicUpdate*/) noexcept override;

private:
    const TWeakPtr<IConnection> Connection_;

};

DEFINE_REFCOUNTED_TYPE(TSyncReplicaCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
