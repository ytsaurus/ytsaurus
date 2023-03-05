#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>
#include <yt/yt/client/table_client/public.h>

#include <yt/yt/core/logging/public.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IBannedReplicaTracker
    : public TRefCounted
{
    virtual bool IsReplicaBanned(TReplicaId replicaId) = 0;
    virtual TError GetReplicaError(TReplicaId replicaId) = 0;
    virtual void BanReplica(TReplicaId replicaId, TError error) = 0;
    virtual void SyncReplicas(const TReplicationCardPtr& replicationCard) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBannedReplicaTracker)

IBannedReplicaTrackerPtr CreateBannedReplicaTracker(NLogging::TLogger logger);

IBannedReplicaTrackerPtr CreateEmptyBannedReplicaTracker();

////////////////////////////////////////////////////////////////////////////////

struct IBannedReplicaTrackerCache
    : public virtual TRefCounted
{
    virtual IBannedReplicaTrackerPtr GetTracker(NTableClient::TTableId tableId) = 0;
    virtual void Reconfigure(const TSlruCacheDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBannedReplicaTrackerCache)

IBannedReplicaTrackerCachePtr CreateBannedReplicaTrackerCache(
    TSlruCacheConfigPtr config,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
