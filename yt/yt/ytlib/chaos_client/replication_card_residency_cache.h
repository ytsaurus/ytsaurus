#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IReplicationCardResidencyCache
    : public virtual TRefCounted
{
    virtual TFuture<NObjectClient::TCellTag> GetReplicationCardResidency(TReplicationCardId replicationCardId) = 0;
    virtual void ForceRefresh(TReplicationCardId replicationCardId, NObjectClient::TCellTag cellTag) = 0;
    virtual void Clear() = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationCardResidencyCache)

////////////////////////////////////////////////////////////////////////////////

IReplicationCardResidencyCachePtr CreateReplicationCardResidencyCache(
    TReplicationCardResidencyCacheConfigPtr config,
    NApi::NNative::IConnectionPtr connection,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
