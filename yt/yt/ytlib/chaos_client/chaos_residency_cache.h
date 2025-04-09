#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/logging/public.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct IChaosResidencyCache
    : public virtual TRefCounted
{
    virtual TFuture<NObjectClient::TCellTag> GetChaosResidency(NObjectClient::TObjectId objectId) = 0;
    virtual void ForceRefresh(NObjectClient::TObjectId objectId, NObjectClient::TCellTag cellTag) = 0;
    virtual void UpdateReplicationCardResidency(NObjectClient::TObjectId objectId, NObjectClient::TCellTag cellTag) = 0;
    virtual void RemoveReplicationCardResidency(NObjectClient::TObjectId objectId) = 0;
    virtual void PingReplicationCardResidency(NObjectClient::TObjectId objectId) = 0;
    virtual void Clear() = 0;
    virtual void Reconfigure(TChaosResidencyCacheConfigPtr config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChaosResidencyCache)

////////////////////////////////////////////////////////////////////////////////

IChaosResidencyCachePtr CreateChaosResidencyCache(
    TChaosResidencyCacheConfigPtr config,
    TChaosCacheChannelConfigPtr chaosCacheChannelConfig,
    NApi::NNative::IConnectionPtr connection,
    NApi::NNative::EChaosResidencyCacheType mode,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
